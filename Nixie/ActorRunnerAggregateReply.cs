
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using DotNext.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Nixie;

/// <summary>
/// Passes a message to the active actor reference making sure only one message is processed at a time.
/// </summary>
/// <typeparam name="TActor"></typeparam>
/// <typeparam name="TRequest"></typeparam>
/// <typeparam name="TResponse"></typeparam>
public sealed class ActorRunnerAggregate<TActor, TRequest, TResponse> : IThreadPoolWorkItem
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
    /// The request/response type actors use an object to assign the response once completed. 
    /// </summary>
    /// <param name="message"></param>
    /// <param name="sender"></param>
    /// <param name="parentReply"></param>
    /// <returns></returns>
    public TaskCompletionSource<TResponse?> SendAndTryDeliver(TRequest message, IGenericActorRef? sender, ActorMessageReply<TRequest, TResponse>? parentReply)
    {
        // A forwarded fire-and-forget message (admitted via TrySend) carries no promise; keep it
        // promise-free through the forward and report the admission outcome on a detached promise,
        // instead of dereferencing the missing parent promise below.
        if (parentReply.HasValue && parentReply.Value.Promise is null)
        {
            TaskCompletionSource<TResponse?> statusPromise = new(TaskCreationOptions.RunContinuationsAsynchronously);

            if (TrySend(message, sender))
                statusPromise.TrySetResult(default);
            else
                statusPromise.TrySetCanceled(CancellationToken.None);

            return statusPromise;
        }

        if (shutdown == 0)
        {
            if (parentReply.HasValue)
            {
                parentReply.Value.Promise!.TrySetCanceled(CancellationToken.None);
                return parentReply.Value.Promise!;
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
            returnPromise = parentReply.Value.Promise!;
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

        // Queued to the pool to keep the drain loop off the sender's thread (a direct call would run
        // Receive synchronously up to its first await on the caller); see IThreadPoolWorkItem.Execute.
        if (1 == Interlocked.Exchange(ref processing, 0))
            ThreadPool.UnsafeQueueUserWorkItem(this, preferLocal: false);

        // If Shutdown() raced with the admission check above, the message may have been enqueued
        // after the shutdown sweep; sweep again so no admitted promise is left uncompleted.
        if (shutdown == 0)
            DrainAndCancelPending();

        return returnPromise;
    }

    /// <summary>
    /// Fire-and-forget admission: enqueues a message without allocating a reply promise and returns whether
    /// it was admitted. Returns <c>false</c> (and enqueues nothing) when the runner is shut down or when the
    /// ordinary inbox is at <c>MaxInboxSize</c>; a <c>false</c> means the message was never delivered, so it
    /// is safe to retry. Control messages are exempt from the bound and are always admitted on a live runner.
    /// Admission is per-message (batching happens at drain time); a promise-free entry simply joins the batch
    /// and is never replied to. Because no promise is allocated, no reject path leaves an unobserved task behind.
    /// </summary>
    /// <param name="message"></param>
    /// <param name="sender"></param>
    /// <returns></returns>
    public bool TrySend(TRequest message, IGenericActorRef? sender)
    {
        if (shutdown == 0)
            return false;

        ActorMessageReply<TRequest, TResponse> messageReply = new(message, sender);

        // Control messages are exempt from maxInboxSize and batched ahead of ordinary messages.
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
                    return false;
                }
            }
            else
            {
                Interlocked.Increment(ref pendingMessageCount);
            }

            inbox.Enqueue(messageReply);
        }

        // Queued to the pool to keep the drain loop off the sender's thread (a direct call would run
        // Receive synchronously up to its first await on the caller); see IThreadPoolWorkItem.Execute.
        if (1 == Interlocked.Exchange(ref processing, 0))
            ThreadPool.UnsafeQueueUserWorkItem(this, preferLocal: false);

        // If Shutdown() raced with the admission check above, the message may have been enqueued
        // after the shutdown sweep; sweep again so no queued message outlives the shutdown.
        if (shutdown == 0)
            DrainAndCancelPending();

        return true;
    }

    /// <summary>
    /// Admission-checked ask: enqueues the message with a reply promise only when it is admitted, and
    /// returns whether it was. Returns <c>false</c> — allocating nothing, neither promise nor exception —
    /// when the runner is shut down or the ordinary inbox is at <c>MaxInboxSize</c>; the message was never
    /// enqueued, so it is safe to retry. Control messages are exempt from the bound and are always admitted
    /// on a live runner. On <c>true</c>, <paramref name="reply"/> completes when the actor processes the
    /// batch containing the message (or as canceled if the actor shuts down before then).
    /// </summary>
    /// <param name="message"></param>
    /// <param name="sender"></param>
    /// <param name="reply"></param>
    /// <returns></returns>
    public bool TryAsk(TRequest message, IGenericActorRef? sender, [NotNullWhen(true)] out Task<TResponse?>? reply)
    {
        if (shutdown == 0)
        {
            reply = null;
            return false;
        }

        // Control messages are exempt from maxInboxSize and batched ahead of ordinary messages.
        bool isControl = isControlMessage is not null && isControlMessage(message);

        if (isControl)
        {
            Interlocked.Increment(ref pendingControlMessageCount);
        }
        else if (maxInboxSize.HasValue)
        {
            int newCount = Interlocked.Increment(ref pendingMessageCount);
            if (newCount > maxInboxSize.Value)
            {
                Interlocked.Decrement(ref pendingMessageCount);
                reply = null;
                return false;
            }
        }
        else
        {
            Interlocked.Increment(ref pendingMessageCount);
        }

        // The promise is allocated only after admission succeeded, so a rejection costs nothing.
        TaskCompletionSource<TResponse?> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        if (isControl)
            controlInbox.Enqueue(new(message, sender, promise));
        else
            inbox.Enqueue(new(message, sender, promise));

        if (1 == Interlocked.Exchange(ref processing, 0))
            ThreadPool.UnsafeQueueUserWorkItem(this, preferLocal: false);

        // If Shutdown() raced with the admission check above, the message may have been enqueued
        // after the shutdown sweep; sweep again so no admitted promise is left uncompleted
        // (the returned task then completes as canceled).
        if (shutdown == 0)
            DrainAndCancelPending();

        reply = promise.Task;
        return true;
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
            DrainAndCancelPending();

            ActorContext?.PostShutdown();
        }

        return success;
    }

    /// <summary>
    /// Sweeps both inboxes once the actor is shut down, cancelling every pending promise so no
    /// Ask caller is left awaiting forever, and keeping the pending counts accurate.
    /// </summary>
    private void DrainAndCancelPending()
    {
        while (TryDequeueNext(out ActorMessageReply<TRequest, TResponse> message, out bool isControl))
        {
            if (isControl)
                Interlocked.Decrement(ref pendingControlMessageCount);
            else
                Interlocked.Decrement(ref pendingMessageCount);

            message.Promise?.TrySetCanceled(CancellationToken.None);
        }
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

        TaskCompletionSource drained = new(TaskCreationOptions.RunContinuationsAsynchronously);

        if (Interlocked.CompareExchange(ref gracefulShutdown, drained, null) is not null)
            return false;

        // The delivery loop may have drained the inboxes and passed its completion check before the
        // signal above was published; without this re-check the caller would wait the full timeout.
        if (IsEmpty)
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
                // Restore the idle latch: the sender flipped it to schedule this turn, and without
                // this a turn that no-ops (actor not wired up yet) would leave the runner unable
                // to ever schedule another turn — the mailbox would fill forever.
                Interlocked.Exchange(ref processing, 1);

                gracefulShutdown?.TrySetResult();
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
                        {
                            // The message was dequeued but will never be batched; cancel its promise
                            // so the caller doesn't await forever.
                            message.Promise?.TrySetCanceled(CancellationToken.None);
                            break;
                        }

                        // The caller cancelled or timed out before this message was batched; its promise is
                        // already completed, so leave it out of the batch (the handler never sees it).
                        // A promise-free (TrySend) message has no promise and is always batched.
                        if (message.Promise is not null && message.Promise.Task.IsCompleted)
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
                            // The handler may have replied to part of the batch before throwing;
                            // TrySetException is a no-op on those, and faults the rest so no
                            // Ask caller is left awaiting forever.
                            foreach (ActorMessageReply<TRequest, TResponse> pending in messages)
                                pending.Promise?.TrySetException(ex);

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

            // A shutdown that interrupted batch collection leaves undelivered messages behind;
            // cancel their promises so no Ask caller is left awaiting forever.
            if (shutdown == 0 && messages.Count > 0)
            {
                foreach (ActorMessageReply<TRequest, TResponse> pending in messages)
                    pending.Promise?.TrySetCanceled(CancellationToken.None);

                messages.Clear();
            }

            // Only signal drain completion when the inboxes are actually empty (or shutdown swept them);
            // on a hand-off break another loop owns the remaining messages and will signal instead.
            if (IsEmpty || shutdown == 0)
                gracefulShutdown?.TrySetResult();
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
