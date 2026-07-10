
using System.Collections.Concurrent;
using DotNext.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Nixie;

/// <summary>
/// Passes a message to the active actor reference making sure only one message is processed at a time.
/// </summary>
/// <typeparam name="TActor"></typeparam>
/// <typeparam name="TRequest"></typeparam>
/// <typeparam name="TResponse"></typeparam>
public sealed class ActorRunnerAggregate<TActor, TRequest, TResponse> 
    where TActor : IActorAggregate<TRequest, TResponse> where TRequest : class where TResponse : class?
{
    private readonly ActorSystem actorSystem;

    private readonly ILogger? logger;

    private readonly int? maxInboxSize;

    private readonly Func<object, bool>? isControlMessage;

    private const int LargeBatchCapacityThreshold = 4096;

    private readonly ConcurrentQueue<ActorMessageReply<TRequest, TResponse>> inbox = new();

    private readonly ConcurrentQueue<ActorMessageReply<TRequest, TResponse>> controlInbox = new();

    private List<ActorMessageReply<TRequest, TResponse>> messages = [];

    private int pendingMessageCount;

    private int pendingControlMessageCount;
    
    private TaskCompletionSource? gracefulShutdown;

    private int processing = 1;

    private int shutdown = 1;

    /// <summary>
    /// The name/id of the actor.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Returns true if the actor's inbox is empty
    /// </summary>
    public bool IsEmpty => inbox.IsEmpty && controlInbox.IsEmpty;

    /// <summary>
    /// Returns the number of messages in the inbox (ordinary + control)
    /// </summary>
    public int MessageCount => Volatile.Read(ref pendingMessageCount) + Volatile.Read(ref pendingControlMessageCount);

    /// <summary>
    /// The reference to the actor.
    /// </summary>
    public IActorAggregate<TRequest, TResponse>? Actor { get; set; }

    /// <summary>
    /// Reference to the current actor context
    /// </summary>
    public ActorAggregateContext<TActor, TRequest, TResponse>? ActorContext { get; set; }

    /// <summary>
    /// True if the actor is processing a message.
    /// </summary>
    public bool IsProcessing => processing == 0;

    /// <summary>
    /// True if the actor is shutdown
    /// </summary>
    public bool IsShutdown => shutdown == 0;

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="actorSystem"></param>
    /// <param name="logger"></param>
    /// <param name="name"></param>
    public ActorRunnerAggregate(ActorSystem actorSystem, ILogger? logger, string name, int? maxInboxSize = null, Func<object, bool>? isControlMessage = null)
    {
        this.actorSystem = actorSystem;
        this.logger = logger;
        this.maxInboxSize = maxInboxSize;
        this.isControlMessage = isControlMessage;

        Name = name;
    }

    /// <summary>
    /// Enqueues a message to the actor and tries to deliver it.
    /// The request/response type actors use an object to assign the response once completed. 
    /// </summary>
    /// <param name="message"></param>
    /// <param name="sender"></param>
    /// <param name="parentReply"></param>
    /// <returns></returns>
    public TaskCompletionSource<TResponse?> SendAndTryDeliver(TRequest message, IGenericActorRef? sender, ActorMessageReply<TRequest, TResponse>? parentReply)
    {
        if (shutdown == 0)
        {
            if (parentReply.HasValue)
            {
                parentReply.Value.Promise.TrySetCanceled(CancellationToken.None);
                return parentReply.Value.Promise;
            }

            TaskCompletionSource<TResponse?> canceledPromise = new(TaskCreationOptions.RunContinuationsAsynchronously);
            canceledPromise.TrySetCanceled(CancellationToken.None);
            return canceledPromise;
        }

        ActorMessageReply<TRequest, TResponse> messageReply;
        TaskCompletionSource<TResponse?> returnPromise;

        if (!parentReply.HasValue)
        {
            TaskCompletionSource<TResponse?> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);
            messageReply = new(message, sender, promise);
            returnPromise = promise;
        }
        else
        {
            messageReply = parentReply.Value;
            returnPromise = parentReply.Value.Promise;
        }

        // Control messages are exempt from maxInboxSize and delivered ahead of ordinary messages, so a
        // completion that resolves an already-admitted request is never rejected.
        if (isControlMessage is not null && isControlMessage(message))
        {
            Interlocked.Increment(ref pendingControlMessageCount);
            controlInbox.Enqueue(messageReply);
        }
        else
        {
            if (maxInboxSize.HasValue)
            {
                int newCount = Interlocked.Increment(ref pendingMessageCount);
                if (newCount > maxInboxSize.Value)
                {
                    Interlocked.Decrement(ref pendingMessageCount);
                    returnPromise.TrySetException(new ActorBusyException(Name, newCount - 1, maxInboxSize.Value));
                    return returnPromise;
                }
            }
            else
            {
                Interlocked.Increment(ref pendingMessageCount);
            }

            inbox.Enqueue(messageReply);
        }

        if (1 == Interlocked.Exchange(ref processing, 0))
            _ = DeliverMessages();

        return returnPromise;
    }

    /// <summary>
    /// Try to shutdown the actor and returns a bool indicating success
    /// </summary>
    /// <returns></returns>
    public bool Shutdown()
    {
        bool success = 1 == Interlocked.Exchange(ref shutdown, 0);

        if (success)
            ActorContext?.PostShutdown();

        return success;
    }

    /// <summary>
    /// Tries to shutdown the actor returns a task whose result confirms shutdown within the specified timespan
    /// </summary>
    /// <param name="maxWait"></param>
    /// <returns></returns>
    public async ValueTask<bool> GracefulShutdown(TimeSpan maxWait)
    {
        if (IsEmpty)
            return Shutdown();

        if (gracefulShutdown is not null)
            return false;

        gracefulShutdown = new(TaskCreationOptions.RunContinuationsAsynchronously);

        Task timeout = Task.Delay(maxWait);

        Task completed = await Task.WhenAny(
            timeout,
            gracefulShutdown.Task
        );
        
        if (completed == timeout)
            Shutdown();

        return completed != timeout;
    }

    /// <summary>
    /// It retrieves a message from the inbox and invokes the actor by passing one message 
    /// at a time until the pending message list is cleared.
    /// </summary>
    /// <returns></returns>
    private async Task DeliverMessages()
    {
        try
        {
            if (Actor is null || ActorContext is null || shutdown == 0)
            {
                gracefulShutdown?.SetResult();
                return;
            }
            
            messages.Clear();

            ActorContext.Runner = this;
            
            while (shutdown == 1)
            {
                do
                {
                    if (shutdown == 0)
                        break;

                    // Control messages are dequeued ahead of ordinary ones, so they land first in the batch
                    // handed to the handler and overtake a backlog of normal requests.
                    while (TryDequeueNext(out ActorMessageReply<TRequest, TResponse> message, out bool isControl))
                    {
                        if (isControl)
                            Interlocked.Decrement(ref pendingControlMessageCount);
                        else
                            Interlocked.Decrement(ref pendingMessageCount);

                        if (shutdown == 0)
                            break;

                        // The caller cancelled or timed out before this message was batched; its promise is
                        // already completed, so leave it out of the batch (the handler never sees it).
                        if (message.Promise.Task.IsCompleted)
                            continue;

                        if (ActorContext is not null)
                        {
                            if (message.Sender is not null)
                                ActorContext.Sender = message.Sender;
                            else
                                ActorContext.Sender = (IGenericActorRef)actorSystem.Nobody;
                        }

                        messages.Add(message);
                    }

                    if (messages.Count > 0 && shutdown == 1)
                    {
                        try
                        {
                            await Actor.Receive(messages);
                        }
                        catch (Exception ex)
                        {
                            logger?.LogError("[{Actor}] {Exception}: {Message}\n{StackTrace}", Name, ex.GetType().Name, ex.Message, ex.StackTrace);
                        }

                        messages.Clear();
                        TrimBatchListIfNeeded();
                    }

                } while (!IsEmpty);

                Interlocked.Exchange(ref processing, 1);

                if (IsEmpty || shutdown == 0)
                    break;

                if (Interlocked.Exchange(ref processing, 0) == 1)
                    continue;

                break;
            }

            gracefulShutdown?.SetResult();
        }
        catch (Exception ex)
        {
            logger?.LogError("[{Actor}] {Exception}: {Message}\n{StackTrace}", Name, ex.GetType().Name, ex.Message, ex.StackTrace);
        }
    }

    /// <summary>
    /// Dequeues the next message to batch, preferring the control queue over the ordinary one so control
    /// messages overtake a backlog of normal requests. Returns false when both queues are empty.
    /// </summary>
    private bool TryDequeueNext(out ActorMessageReply<TRequest, TResponse> message, out bool isControl)
    {
        if (controlInbox.TryDequeue(out message))
        {
            isControl = true;
            return true;
        }

        if (inbox.TryDequeue(out message))
        {
            isControl = false;
            return true;
        }

        isControl = false;
        return false;
    }

    private void TrimBatchListIfNeeded()
    {
        if (messages.Capacity > LargeBatchCapacityThreshold)
            messages = [];
    }
}
