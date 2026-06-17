
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Nixie;

/// <summary>
/// Passes a struct message to the active actor reference making sure only one message is processed at a time.
/// </summary>
/// <typeparam name="TActor"></typeparam>
/// <typeparam name="TRequest"></typeparam>
public sealed class ActorRunnerStruct<TActor, TRequest> where TActor : IActorStruct<TRequest> where TRequest : struct
{
    private readonly ActorSystem actorSystem;

    private readonly ILogger? logger;

    private readonly int? maxInboxSize;

    private readonly ConcurrentQueue<ActorMessage<TRequest>> inbox = new();

    private int pendingMessageCount;

    private TaskCompletionSource? gracefulShutdown;

    private int processing = 1;

    private int shutdown = 1;

    /// <summary>
    /// Returns the name of the actor
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Returns true if the actor's inbox is empty
    /// </summary>
    public bool IsEmpty => inbox.IsEmpty;

    /// <summary>
    /// Returns the number of messages in the inbox
    /// </summary>
    public int MessageCount => Volatile.Read(ref pendingMessageCount);

    /// <summary>
    /// Reference to the actual actor
    /// </summary>
    public IActorStruct<TRequest>? Actor { get; set; }

    /// <summary>
    /// Reference to the current actor context
    /// </summary>
    public ActorContextStruct<TActor, TRequest>? ActorContext { get; set; }

    /// <summary>
    /// Returns true if the runner is processing messages
    /// </summary>
    public bool IsProcessing => processing == 0;

    /// <summary>
    /// Returns true if the actor is shutdown
    /// </summary>
    public bool IsShutdown => shutdown == 0;

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="actorSystem"></param>
    /// <param name="logger"></param>
    /// <param name="name"></param>
    public ActorRunnerStruct(ActorSystem actorSystem, ILogger? logger, string name, int? maxInboxSize = null)
    {
        this.actorSystem = actorSystem;
        this.logger = logger;
        this.maxInboxSize = maxInboxSize;

        Name = name;
    }

    /// <summary>
    /// Enqueues a message to the actor and tries to deliver it.
    /// </summary>
    /// <param name="message"></param>
    /// <param name="sender"></param>
    public void SendAndTryDeliver(TRequest message, IGenericActorRef? sender)
    {
        if (shutdown == 0)
            return;

        if (maxInboxSize.HasValue)
        {
            int newCount = Interlocked.Increment(ref pendingMessageCount);
            if (newCount > maxInboxSize.Value)
            {
                Interlocked.Decrement(ref pendingMessageCount);
                throw new ActorBusyException(Name, newCount - 1, maxInboxSize.Value);
            }
        }
        else
        {
            Interlocked.Increment(ref pendingMessageCount);
        }

        inbox.Enqueue(new(message, sender));

        if (1 == Interlocked.Exchange(ref processing, 0))
            _ = DeliverMessages();
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
        if (inbox.IsEmpty)
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
    /// Enqueues a message to the actor and tries to deliver it.
    /// The request/response type actors use an object to assign the response once completed.    
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
                while (inbox.TryDequeue(out ActorMessage<TRequest> message))
                {
                    Interlocked.Decrement(ref pendingMessageCount);

                    if (shutdown == 0)
                        break;

                    if (ActorContext is not null)
                    {
                        if (message.Sender is not null)
                            ActorContext.Sender = message.Sender;
                        else
                            ActorContext.Sender = (IGenericActorRef)actorSystem.Nobody;
                    }

                    try
                    {
                        await Actor.Receive(message.Request);
                    }
                    catch (Exception ex)
                    {
                        logger?.LogError("[{Actor}] {Exception}: {Message}\n{StackTrace}", Name, ex.GetType().Name, ex.Message, ex.StackTrace);
                    }
                }

                Interlocked.Exchange(ref processing, 1);

                if (inbox.IsEmpty || shutdown == 0)
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
    /// Allows to peek at the next message in the inbox without removing it.
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public bool TryPeek(out TRequest? message)
    {
        if (inbox.TryPeek(out ActorMessage<TRequest> nextMssage))
        {
            message = nextMssage.Request;
            return true;
        }

        message = null;
        return false;
    }
    
    /// <summary>
    /// Allows to dequeue the next message in the inbox.
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public bool TryDequeue(out TRequest? message)
    {
        if (inbox.TryDequeue(out ActorMessage<TRequest> nextMssage))
        {
            Interlocked.Decrement(ref pendingMessageCount);
            message = nextMssage.Request;
            return true;
        }

        message = null;
        return false;
    }
}
