
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
public sealed class ActorRunner<TActor, TRequest, TResponse> : IThreadPoolWorkItem where TActor : IActor<TRequest, TResponse> where TRequest : class where TResponse : class?
{
    private readonly ActorSystem actorSystem;

    private readonly ILogger? logger;

    private readonly int? maxInboxSize;

    private readonly Func<object, bool>? isControlMessage;

    private readonly ConcurrentQueue<ActorMessageReply<TRequest, TResponse>> inbox = new();

    // Allocated only when a classifier exists: an empty ConcurrentQueue eagerly builds its lock and
    // first segment (about 2 KB for this envelope type), and an actor without a classifier can never
    // put a message in it. Non-null exactly when isControlMessage is non-null; every access relies
    // on that invariant.
    private readonly ConcurrentQueue<ActorMessageReply<TRequest, TResponse>>? controlInbox;

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
    public bool IsEmpty => inbox.IsEmpty && (controlInbox is null || controlInbox.IsEmpty);

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

        // See the field declaration: no classifier means no control message can ever be enqueued.
        controlInbox = isControlMessage is not null ? new() : null;

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
        // A forwarded message with no promise (fire-and-forget, or a pooled ask whose reply travels on
        // its handle) keeps its own reply channel through the forward. This overload must still return a
        // task completion source, so the admission outcome goes on a detached promise. Callers that do
        // not need that outcome must use Forward, which allocates nothing here.
        if (parentReply.HasValue && parentReply.Value.Promise is null)
        {
            TaskCompletionSource<TResponse?> statusPromise = new(TaskCreationOptions.RunContinuationsAsynchronously);

            if (ForwardWithoutPromise(message, sender, parentReply.Value))
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
            controlInbox!.Enqueue(messageReply);
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
            ThreadPool.UnsafeQueueUserWorkItem(this, preferLocal: false);

        // If Shutdown() raced with the admission check above, the message may have been enqueued
        // after the shutdown sweep; sweep again so no admitted promise is left uncompleted.
        if (shutdown == 0)
            DrainAndCancelPending();

        return returnPromise;
    }

    /// <summary>
    /// Forwards a message to this runner on behalf of a parent envelope, without allocating anything for
    /// the forward itself. A router uses this to hand its own message to a routee: the routee replies to
    /// the original caller, so the forward needs no promise of its own.
    ///
    /// Each of the three envelope kinds keeps its own reply channel. An ordinary ask carries its parent
    /// promise through, so the routee completes the caller's task. A pooled ask carries its parent handle
    /// through, so the routee completes the caller's pooled reply. A promise-free message carries neither
    /// and is simply admitted. A parent envelope of <c>null</c> is forwarded as a promise-free message.
    /// </summary>
    /// <param name="message"></param>
    /// <param name="sender"></param>
    /// <param name="parentReply"></param>
    public void Forward(TRequest message, IGenericActorRef? sender, ActorMessageReply<TRequest, TResponse>? parentReply)
    {
        if (!parentReply.HasValue)
        {
            TrySend(message, sender);
            return;
        }

        // The parent promise is reused, not duplicated, so this path allocates no promise either.
        if (parentReply.Value.Promise is not null)
        {
            SendAndTryDeliver(message, sender, parentReply);
            return;
        }

        ForwardWithoutPromise(message, sender, parentReply.Value);
    }

    /// <summary>
    /// Forwards an envelope whose Promise is null. That covers two different kinds: a pooled ask, whose
    /// reply travels on its handle, and a genuinely promise-free message. Treating both as promise-free
    /// dropped the pooled handle and left the original caller awaiting a reply forever.
    /// </summary>
    private bool ForwardWithoutPromise(TRequest message, IGenericActorRef? sender, ActorMessageReply<TRequest, TResponse> parentReply)
    {
        if (parentReply.PooledHandle.IsDefault)
            return TrySend(message, sender);

        // The parent's sender is kept when the forward does not name one, which is what the promise
        // path does: it reuses the whole parent envelope, sender included.
        return TrySendPooled(message, sender ?? parentReply.Sender, parentReply.PooledHandle);
    }

    /// <summary>
    /// Admits a message that carries an existing pooled reply handle instead of a promise. The admission
    /// rules match <see cref="TrySend"/>. A rejection completes the handle, because the original caller
    /// awaits that handle and no later delivery can complete it: a shut-down runner cancels it, and a full
    /// inbox faults it with <see cref="ActorBusyException"/>, which mirrors the promise-carrying path.
    /// </summary>
    private bool TrySendPooled(TRequest message, IGenericActorRef? sender, ReplyHandle<TResponse> pooledHandle)
    {
        if (shutdown == 0)
        {
            pooledHandle.TrySetCanceled();
            return false;
        }

        ActorMessageReply<TRequest, TResponse> messageReply = new(message, sender, pooledHandle);

        ConcurrentQueue<ActorMessageReply<TRequest, TResponse>>? control = controlInbox;

        // Control messages are exempt from maxInboxSize and delivered ahead of ordinary messages.
        if (control is not null && isControlMessage!(message))
        {
            Interlocked.Increment(ref pendingControlMessageCount);
            control.Enqueue(messageReply);
        }
        else
        {
            if (maxInboxSize.HasValue)
            {
                int newCount = Interlocked.Increment(ref pendingMessageCount);
                if (newCount > maxInboxSize.Value)
                {
                    Interlocked.Decrement(ref pendingMessageCount);
                    pooledHandle.TrySetException(new ActorBusyException(Name, newCount - 1, maxInboxSize.Value));
                    return false;
                }
            }
            else
            {
                Interlocked.Increment(ref pendingMessageCount);
            }

            inbox.Enqueue(messageReply);
        }

        if (1 == Interlocked.Exchange(ref processing, 0))
            ThreadPool.UnsafeQueueUserWorkItem(this, preferLocal: false);

        // If Shutdown() raced with the admission check above, the message may have been enqueued
        // after the shutdown sweep; sweep again so no admitted handle is left uncompleted.
        if (shutdown == 0)
            DrainAndCancelPending();

        return true;
    }

    /// <summary>
    /// Fire-and-forget admission: enqueues a message without allocating a reply promise and returns whether
    /// it was admitted. Returns <c>false</c> (and enqueues nothing) when the runner is shut down or when the
    /// ordinary inbox is at <c>MaxInboxSize</c>; a <c>false</c> means the message was never delivered, so it
    /// is safe to retry. Control messages are exempt from the bound and are always admitted on a live runner.
    /// Because no promise is allocated, no reject path leaves an unobserved faulted/canceled task behind.
    /// </summary>
    /// <param name="message"></param>
    /// <param name="sender"></param>
    /// <returns></returns>
    public bool TrySend(TRequest message, IGenericActorRef? sender)
    {
        if (shutdown == 0)
            return false;

        ActorMessageReply<TRequest, TResponse> messageReply = new(message, sender);

        // Control messages are exempt from maxInboxSize and delivered ahead of ordinary messages.
        if (isControlMessage is not null && isControlMessage(message))
        {
            Interlocked.Increment(ref pendingControlMessageCount);
            controlInbox!.Enqueue(messageReply);
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
    /// message (or as canceled if the actor shuts down before then).
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

        // Control messages are exempt from maxInboxSize and delivered ahead of ordinary messages.
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
            controlInbox!.Enqueue(new(message, sender, promise));
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
    /// Admission-checked ask on the pooled reply path: same admission contract as
    /// <see cref="TryAsk"/>, but the reply is a <see cref="ValueTask{TResponse}"/> backed by a pooled
    /// <see cref="ReplyPromise{TResponse}"/> instead of a freshly allocated task. The returned
    /// ValueTask must be awaited exactly once; consuming it recycles the promise for a later ask.
    /// A rejection allocates nothing and enqueues nothing, so it is safe to retry.
    /// </summary>
    /// <param name="message"></param>
    /// <param name="sender"></param>
    /// <param name="reply"></param>
    /// <returns></returns>
    public bool TryAskPooled(TRequest message, IGenericActorRef? sender, out ValueTask<TResponse?> reply)
    {
        if (shutdown == 0)
        {
            reply = default;
            return false;
        }

        // Control messages are exempt from maxInboxSize and delivered ahead of ordinary messages.
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
                reply = default;
                return false;
            }
        }
        else
        {
            Interlocked.Increment(ref pendingMessageCount);
        }

        // The promise is rented only after admission succeeded, so a rejection costs nothing. The
        // handle and the awaitable are both captured before the message (and with it the promise)
        // is published to the delivery loop.
        ReplyPromise<TResponse> promise = ReplyPromise<TResponse>.Rent();
        ReplyHandle<TResponse> handle = promise.Handle;
        reply = promise.AsValueTask();

        if (isControl)
            controlInbox!.Enqueue(new(message, sender, handle));
        else
            inbox.Enqueue(new(message, sender, handle));

        if (1 == Interlocked.Exchange(ref processing, 0))
            ThreadPool.UnsafeQueueUserWorkItem(this, preferLocal: false);

        // If Shutdown() raced with the admission check above, the message may have been enqueued
        // after the shutdown sweep; sweep again so no admitted promise is left uncompleted
        // (the returned ValueTask then completes as canceled).
        if (shutdown == 0)
            DrainAndCancelPending();

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
            message.PooledHandle.TrySetCanceled();
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

        // WaitAsync releases its timer as soon as the drain signal arrives. A Task.WhenAny over a
        // Task.Delay leaves that timer registered until the full deadline expires, so every actor
        // that drains early keeps a live timer for the rest of its timeout.
        bool drainedInTime;

        try
        {
            await drained.Task.WaitAsync(maxWait);
            drainedInTime = true;
        }
        catch (TimeoutException)
        {
            drainedInTime = false;
        }

        // Shutdown in both outcomes: a drained inbox must still stop the actor (reject further
        // sends and run PostShutdown), and a timeout forces the stop.
        Shutdown();

        return drainedInTime;
    }

    /// <summary>
    /// Dequeues the next message to process, preferring the control queue over the ordinary one so control
    /// messages overtake a backlog of normal requests. Returns false when both queues are empty.
    /// </summary>
    private bool TryDequeueNext(out ActorMessageReply<TRequest, TResponse> message, out bool isControl)
    {
        if (controlInbox is not null && controlInbox.TryDequeue(out message))
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
                // Control messages are drained ahead of ordinary ones on every iteration, so a completion
                // overtakes any backlog of normal requests. FIFO is preserved within each class.
                while (TryDequeueNext(out ActorMessageReply<TRequest, TResponse> message, out bool isControl))
                {
                    if (isControl)
                        Interlocked.Decrement(ref pendingControlMessageCount);
                    else
                        Interlocked.Decrement(ref pendingMessageCount);

                    if (shutdown == 0 || ActorContext is null)
                    {
                        // The message was dequeued but will never be delivered; cancel its promise
                        // so the caller doesn't await forever.
                        message.Promise?.TrySetCanceled(CancellationToken.None);
                        message.PooledHandle.TrySetCanceled();
                        break;
                    }

                    // The caller cancelled or timed out before this message reached the head of the
                    // queue; its reply is already completed, so skip delivery (the handler never runs).
                    // A promise-free (TrySend) message has neither reply channel and is always delivered.
                    if (message.Promise is not null && message.Promise.Task.IsCompleted)
                        continue;

                    if (!message.PooledHandle.IsDefault && message.PooledHandle.IsCompleted)
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
                        {
                            message.Promise?.TrySetResult(response);
                            message.PooledHandle.TrySetResult(response);
                        }
                    }
                    catch (Exception ex)
                    {
                        message.Promise?.TrySetException(ex);
                        message.PooledHandle.TrySetException(ex);

                        // The arguments (ex.StackTrace formats a whole stack) are built only when the level

                        // is actually enabled; a non-null logger with error logging off paid for them before.

                        if (logger is not null && logger.IsEnabled(LogLevel.Error))

                            logger.LogError("[{Actor}] {Exception}: {Message}\n{StackTrace}", Name, ex.GetType().Name, ex.Message, ex.StackTrace);
                    }
                }

                Interlocked.Exchange(ref processing, 1);

                if (IsEmpty || shutdown == 0)
                    break;

                if (Interlocked.Exchange(ref processing, 0) == 1)
                    continue;

                break;
            }

            // Only signal drain completion when the inboxes are actually empty (or shutdown swept them);
            // on a hand-off break another loop owns the remaining messages and will signal instead.
            if (IsEmpty || shutdown == 0)
                gracefulShutdown?.TrySetResult();
        }
        catch (Exception ex)
        {
            // The arguments (ex.StackTrace formats a whole stack) are built only when the level
            // is actually enabled; a non-null logger with error logging off paid for them before.
            if (logger is not null && logger.IsEnabled(LogLevel.Error))
                logger.LogError("[{Actor}] {Exception}: {Message}\n{StackTrace}", Name, ex.GetType().Name, ex.Message, ex.StackTrace);
            
            //Console.Error.WriteLine(ex.Message);
        }
    }
}
