
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Nixie;

/// <summary>
/// Passes a message to the active actor reference making sure only one message is processed at a time.
/// </summary>
/// <typeparam name="TActor"></typeparam>
/// <typeparam name="TRequest"></typeparam>
public sealed class ActorRunnerAggregate<TActor, TRequest> : IThreadPoolWorkItem where TActor : IActorAggregate<TRequest> where TRequest : class
{
    private readonly ActorSystem actorSystem;

    private readonly ILogger? logger;

    private readonly int? maxInboxSize;

    private const int LargeBatchCapacityThreshold = 4096;

    private readonly ConcurrentQueue<ActorMessage<TRequest>> inbox = new();
    
    private List<TRequest> messages = [];

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
    public IActorAggregate<TRequest>? Actor { get; set; }

    /// <summary>
    /// Reference to the current actor context
    /// </summary>
    public ActorAggregateContext<TActor, TRequest>? ActorContext { get; set; }

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
    public ActorRunnerAggregate(ActorSystem actorSystem, ILogger? logger, string name, int? maxInboxSize = null)
    {
        this.actorSystem = actorSystem;
        this.logger = logger;
        this.maxInboxSize = maxInboxSize;

        Name = name;
    }

    /// <summary>
    /// Thread-pool wakeup entry point: starts one drain turn. Scheduled with
    /// ThreadPool.UnsafeQueueUserWorkItem so the wakeup captures no ExecutionContext — Task.Run would
    /// inflate every wakeup task with the sender's captured context (a ContingentProperties allocation
    /// per wakeup whenever an AsyncLocal is live) and leak the triggering sender's context into other
    /// senders' message processing.
    /// </summary>
    void IThreadPoolWorkItem.Execute()
    {
        _ = DeliverMessages();
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

        inbox.Enqueue(new ActorMessage<TRequest>(message, sender));

        // Queued to the pool to keep the drain loop off the sender's thread (a direct call would run
        // Receive synchronously up to its first await on the caller); see IThreadPoolWorkItem.Execute.
        if (1 == Interlocked.Exchange(ref processing, 0))
            ThreadPool.UnsafeQueueUserWorkItem(this, preferLocal: false);

        // If Shutdown() raced with the admission check above, the message may have been enqueued
        // after the shutdown sweep; sweep again so the pending count stays accurate.
        if (shutdown == 0)
            DrainPendingMessages();
    }

    /// <summary>
    /// Try to shutdown the actor and returns a bool indicating success
    /// </summary>
    /// <returns></returns>
    public bool Shutdown()
    {
        bool success = 1 == Interlocked.Exchange(ref shutdown, 0);

        if (success)
        {
            DrainPendingMessages();

            ActorContext?.PostShutdown();
        }

        return success;
    }

    /// <summary>
    /// Discards any messages still queued once the actor is shut down so the pending count
    /// stays accurate and queued requests are released.
    /// </summary>
    private void DrainPendingMessages()
    {
        while (inbox.TryDequeue(out _))
            Interlocked.Decrement(ref pendingMessageCount);
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

        TaskCompletionSource drained = new(TaskCreationOptions.RunContinuationsAsynchronously);

        if (Interlocked.CompareExchange(ref gracefulShutdown, drained, null) is not null)
            return false;

        // The delivery loop may have drained the inbox and passed its completion check before the
        // signal above was published; without this re-check the caller would wait the full timeout.
        if (inbox.IsEmpty)
        {
            Shutdown();
            return true;
        }

        Task timeout = Task.Delay(maxWait);

        Task completed = await Task.WhenAny(
            timeout,
            drained.Task
        );

        // Shutdown in both outcomes: a drained inbox must still stop the actor (reject further
        // sends and run PostShutdown), and a timeout forces the stop.
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
                // Restore the idle latch: the sender flipped it to schedule this turn, and without
                // this a turn that no-ops (actor not wired up yet) would leave the runner unable
                // to ever schedule another turn — the mailbox would fill forever.
                Interlocked.Exchange(ref processing, 1);

                gracefulShutdown?.TrySetResult();
                return;
            }

            ActorContext.Runner = this;

            while (shutdown == 1)
            {
                do
                {
                    if (shutdown == 0)
                        break;

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

                        messages.Add(message.Request);
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

                } while (!inbox.IsEmpty);

                Interlocked.Exchange(ref processing, 1);

                if (inbox.IsEmpty || shutdown == 0)
                    break;

                if (Interlocked.Exchange(ref processing, 0) == 1)
                    continue;

                break;
            }

            // A shutdown that interrupted batch collection leaves undelivered requests behind;
            // release them so they aren't pinned for the runner's lifetime.
            if (shutdown == 0 && messages.Count > 0)
                messages.Clear();

            // Only signal drain completion when the inbox is actually empty (or shutdown swept it);
            // on a hand-off break another loop owns the remaining messages and will signal instead.
            if (inbox.IsEmpty || shutdown == 0)
                gracefulShutdown?.TrySetResult();
        }
        catch (Exception ex)
        {
            logger?.LogError("[{Actor}] {Exception}: {Message}\n{StackTrace}", Name, ex.GetType().Name, ex.Message, ex.StackTrace);
            
            // Console.WriteLine("[{0}] {1}: {2}\n{3}", Name, ex.GetType().Name, ex.Message, ex.StackTrace);
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

    private void TrimBatchListIfNeeded()
    {
        if (messages.Capacity > LargeBatchCapacityThreshold)
            messages = [];
    }
}
