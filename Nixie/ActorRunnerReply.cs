
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
public sealed class ActorRunner<TActor, TRequest, TResponse> where TActor : IActor<TRequest, TResponse> where TRequest : class where TResponse : class?
{
    private readonly ActorSystem actorSystem;

    private readonly ILogger? logger;

    private readonly int? maxInboxSize;

    private readonly Func<object, bool>? isControlMessage;

    private readonly ConcurrentQueue<ActorMessageReply<TRequest, TResponse>> inbox = new();

    private readonly ConcurrentQueue<ActorMessageReply<TRequest, TResponse>> controlInbox = new();

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
    public IActor<TRequest, TResponse>? Actor { get; set; }

    /// <summary>
    /// Reference to the current actor context
    /// </summary>
    public ActorContext<TActor, TRequest, TResponse>? ActorContext { get; set; }

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
    public ActorRunner(ActorSystem actorSystem, ILogger? logger, string name, int? maxInboxSize = null, Func<object, bool>? isControlMessage = null)
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
            Task.Run(DeliverMessages);

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
    /// Dequeues the next message to process, preferring the control queue over the ordinary one so control
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

            ActorContext.Runner = this;

            while (shutdown == 1)
            {
                // Control messages are drained ahead of ordinary ones on every iteration, so a completion
                // overtakes any backlog of normal requests. FIFO is preserved within each class.
                while (TryDequeueNext(out ActorMessageReply<TRequest, TResponse> message, out bool isControl))
                {
                    if (isControl)
                        Interlocked.Decrement(ref pendingControlMessageCount);
                    else
                        Interlocked.Decrement(ref pendingMessageCount);

                    if (shutdown == 0 || ActorContext is null)
                        break;

                    // The caller cancelled or timed out before this message reached the head of the
                    // queue; its promise is already completed, so skip delivery (the handler never runs).
                    if (message.Promise.Task.IsCompleted)
                        continue;

                    if (message.Sender is not null)
                        ActorContext.Sender = message.Sender;
                    else
                        ActorContext.Sender = (IGenericActorRef)actorSystem.Nobody;

                    ActorContext.Reply = message;
                    ActorContext.ByPassReply = false;

                    try
                    {
                        TResponse? response = await Actor.Receive(message.Request);

                        if (!ActorContext.ByPassReply)
                            message.Promise.TrySetResult(response);
                    }
                    catch (Exception ex)
                    {
                        message.Promise.TrySetException(ex);

                        logger?.LogError("[{Actor}] {Exception}: {Message}\n{StackTrace}", Name, ex.GetType().Name, ex.Message, ex.StackTrace);
                    }
                }

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
            
            //Console.Error.WriteLine(ex.Message);
        }
    }
    
    /// <summary>
    /// It retrieves a message from the inbox and invokes the actor by passing one message 
    /// at a time until the pending message list is cleared.
    /// </summary>
    /// <returns></returns>
    private async Task DeliverSingleMessage(ActorMessageReply<TRequest, TResponse> singleMessage)
    {
        try
        {
            if (Actor is null || ActorContext is null || shutdown == 0)
            {
                gracefulShutdown?.SetResult();
                return;
            }

            ActorContext.Runner = this;
            
            if (singleMessage.Sender is not null)
                ActorContext.Sender = singleMessage.Sender;
            else
                ActorContext.Sender = (IGenericActorRef)actorSystem.Nobody;

            ActorContext.Reply = singleMessage;
            ActorContext.ByPassReply = false;

            try
            {
                TResponse? response = await Actor.Receive(singleMessage.Request);

                if (!ActorContext.ByPassReply)
                    singleMessage.Promise.TrySetResult(response);
            }
            catch (Exception ex)
            {
                singleMessage.Promise.TrySetException(ex);

                logger?.LogError("[{Actor}] {Exception}: {Message}\n{StackTrace}", Name, ex.GetType().Name, ex.Message, ex.StackTrace);
            }

            do
            {
                while (inbox.TryDequeue(out ActorMessageReply<TRequest, TResponse> message))
                {
                    if (shutdown == 0 || ActorContext is null)
                        break;

                    if (message.Sender is not null)
                        ActorContext.Sender = message.Sender;
                    else
                        ActorContext.Sender = (IGenericActorRef)actorSystem.Nobody;

                    ActorContext.Reply = message;
                    ActorContext.ByPassReply = false;

                    try
                    {
                        TResponse? response = await Actor.Receive(message.Request);

                        if (!ActorContext.ByPassReply)
                            message.Promise.TrySetResult(response);
                    }
                    catch (Exception ex)
                    {
                        message.Promise.TrySetException(ex);

                        logger?.LogError("[{Actor}] {Exception}: {Message}\n{StackTrace}", Name, ex.GetType().Name, ex.Message, ex.StackTrace);
                    }
                }
            } while (shutdown == 1 && (Interlocked.CompareExchange(ref processing, 1, 0) != 0));

            gracefulShutdown?.SetResult();
        }
        catch (Exception ex)
        {
            logger?.LogError("[{Actor}] {Exception}: {Message}\n{StackTrace}", Name, ex.GetType().Name, ex.Message, ex.StackTrace);
            
            // Console.Error.WriteLine(ex.Message);
        }
    }

    private async Task DeliverMessageInternal(
        ActorContext<TActor, TRequest, TResponse> actorContext, 
        IActor<TRequest, TResponse> actor,
        ActorMessageReply<TRequest, TResponse> message
    )
    {
        if (message.Sender is not null)
            actorContext.Sender = message.Sender;
        else
            actorContext.Sender = (IGenericActorRef)actorSystem.Nobody;

        actorContext.Reply = message;
        actorContext.ByPassReply = false;

        try
        {
            TResponse? response = await actor.Receive(message.Request);

            if (!actorContext.ByPassReply)
                message.Promise.TrySetResult(response);
        }
        catch (Exception ex)
        {
            message.Promise.TrySetException(ex);

            logger?.LogError("[{Actor}] {Exception}: {Message}\n{StackTrace}", Name, ex.GetType().Name, ex.Message, ex.StackTrace);
        }
    }
}
