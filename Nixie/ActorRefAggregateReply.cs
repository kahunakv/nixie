using System.Runtime.CompilerServices;

using System.Diagnostics.CodeAnalysis;

namespace Nixie;

/// <summary>
/// Represents an actor reference.
/// </summary>
/// <typeparam name="TActor"></typeparam>
/// <typeparam name="TRequest"></typeparam>
/// <typeparam name="TResponse"></typeparam>
public sealed class ActorRefAggregate<TActor, TRequest, TResponse> : IGenericActorRef, IActorRefAggregate<TActor, TRequest, TResponse>
    where TActor : IActorAggregate<TRequest, TResponse> where TRequest : class where TResponse : class?
{
    /// <summary>
    /// Returns a reference to the actor's runner
    /// </summary>
    public ActorRunnerAggregate<TActor, TRequest, TResponse> Runner { get; }

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="runner"></param>
    public ActorRefAggregate(ActorRunnerAggregate<TActor, TRequest, TResponse> runner)
    {
        Runner = runner;
    }

    /// <summary>
    /// Passes a message to the actor without expecting a response and without specifying a sender.
    /// Uses the promise-free admission path, so no reply promise is allocated and a rejected message (full
    /// bounded inbox, or a shut-down runner) is dropped without leaving an unobserved task behind.
    /// </summary>
    /// <param name="message"></param>
    public void Send(TRequest message)
    {
        Runner.TrySend(message, null);
    }

    /// <summary>
    /// Passes a message to the actor without expecting a response and specifying a sender.
    /// Uses the promise-free admission path (see <see cref="Send(TRequest)"/>).
    /// </summary>
    /// <param name="message"></param>
    /// <param name="sender"></param>
    public void Send(TRequest message, IGenericActorRef sender)
    {
        Runner.TrySend(message, sender);
    }

    /// <summary>
    /// Passes a message to the actor without expecting a response and specifying a parent promise
    /// </summary>
    /// <param name="message"></param>
    /// <param name="parentPromise"></param>
    public void Send(TRequest message, ActorMessageReply<TRequest, TResponse>? parentPromise)
    {
        Runner.Forward(message, null, parentPromise);
    }

    /// <summary>
    /// Fire-and-forget send that returns its admission result. Returns <c>true</c> when the message was
    /// enqueued (ordinary or control inbox) and <c>false</c> when it was rejected and never processed — the
    /// ordinary inbox was at <c>MaxInboxSize</c>, or the runner is shut down. A <c>false</c> is safe to retry:
    /// the message was never delivered. Unlike <see cref="Send(TRequest)"/>, no reply promise is allocated, so
    /// a rejection leaves no unobserved task behind.
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public bool TrySend(TRequest message)
    {
        return Runner.TrySend(message, null);
    }

    /// <summary>
    /// Fire-and-forget send with an explicit sender that returns its admission result.
    /// See <see cref="TrySend(TRequest)"/>.
    /// </summary>
    /// <param name="message"></param>
    /// <param name="sender"></param>
    /// <returns></returns>
    public bool TrySend(TRequest message, IGenericActorRef sender)
    {
        return Runner.TrySend(message, sender);
    }

    /// <summary>
    /// Admission-checked ask: returns <c>true</c> with a <paramref name="reply"/> task that completes when
    /// the actor processes the batch containing the message, or <c>false</c> when the message was rejected
    /// (bounded inbox at capacity, or the runner shut down) and never enqueued, so it is safe to retry.
    /// Unlike <see cref="Ask(TRequest)"/>, a rejection allocates nothing — no promise and no
    /// <see cref="ActorBusyException"/> — making this the cheap path for busy/retry loops.
    /// </summary>
    /// <param name="message"></param>
    /// <param name="reply"></param>
    /// <returns></returns>
    public bool TryAsk(TRequest message, [NotNullWhen(true)] out Task<TResponse?>? reply)
    {
        return Runner.TryAsk(message, null, out reply);
    }

    /// <summary>
    /// Admission-checked ask with an explicit sender. See <see cref="TryAsk(TRequest, out Task{TResponse})"/>.
    /// </summary>
    /// <param name="message"></param>
    /// <param name="sender"></param>
    /// <param name="reply"></param>
    /// <returns></returns>
    public bool TryAsk(TRequest message, IGenericActorRef sender, [NotNullWhen(true)] out Task<TResponse?>? reply)
    {
        return Runner.TryAsk(message, sender, out reply);
    }

    /// <summary>
    /// Sends a message to actor expecting a response and without specifying a sender
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>    
    public Task<TResponse?> Ask(TRequest message)
    {
        // Non-async: return the promise's own task directly, avoiding the extra async state-machine box and
        // wrapper task. A synchronous fault from SendAndTryDeliver (e.g. a throwing control predicate) is
        // still surfaced through the returned task, matching the previous async behavior.
        try
        {
            return Runner.SendAndTryDeliver(message, null, null).Task;
        }
        catch (Exception exception)
        {
            return Task.FromException<TResponse?>(exception);
        }
    }

    /// <summary>
    /// Sends a message to the actor and expects a response
    /// An exception will be thrown if the timeout limit is reached
    /// </summary>
    /// <param name="message"></param>
    /// <param name="timeout"></param>
    /// <returns></returns>
    /// <exception cref="AskTimeoutException"></exception>
    public async Task<TResponse?> Ask(TRequest message, TimeSpan timeout)
    {
        TaskCompletionSource<TResponse?> promise = Runner.SendAndTryDeliver(message, null, null);

        using CancellationTokenSource timeoutCancellationTokenSource = new(timeout);

        CancellationTokenRegistration registration = timeoutCancellationTokenSource.Token.Register(
            static (state, token) => ((TaskCompletionSource<TResponse?>)state!).TrySetCanceled(token),
            promise
        );

        try
        {
            return await promise.Task;
        }
        catch (OperationCanceledException ex) when (ex.CancellationToken == timeoutCancellationTokenSource.Token)
        {
            throw new AskTimeoutException($"Timeout after {timeout} waiting for a reply");
        }
        finally
        {
            registration.Dispose();
        }
    }

    /// <summary>
    /// Sends a message to actor expecting a response and specifying the sender
    /// </summary>
    /// <param name="message"></param>
    /// <param name="sender"></param>
    /// <returns></returns>
    public Task<TResponse?> Ask(TRequest message, IGenericActorRef sender)
    {
        // Non-async direct return; see the no-sender overload for rationale.
        try
        {
            return Runner.SendAndTryDeliver(message, sender, null).Task;
        }
        catch (Exception exception)
        {
            return Task.FromException<TResponse?>(exception);
        }
    }

    /// <summary>
    /// Sends a message to actor expecting a response and specifying the sender
    /// An exception will be thrown if the timeout limit is reached
    /// </summary>
    /// <param name="message"></param>
    /// <param name="sender"></param>
    /// <param name="timeout"></param>
    /// <returns></returns>
    /// <exception cref="AskTimeoutException"></exception>
    public async Task<TResponse?> Ask(TRequest message, IGenericActorRef sender, TimeSpan timeout)
    {
        TaskCompletionSource<TResponse?> promise = Runner.SendAndTryDeliver(message, sender, null);

        using CancellationTokenSource timeoutCancellationTokenSource = new(timeout);

        CancellationTokenRegistration registration = timeoutCancellationTokenSource.Token.Register(
            static (state, token) => ((TaskCompletionSource<TResponse?>)state!).TrySetCanceled(token),
            promise
        );

        try
        {
            return await promise.Task;
        }
        catch (OperationCanceledException ex) when (ex.CancellationToken == timeoutCancellationTokenSource.Token)
        {
            throw new AskTimeoutException($"Timeout after {timeout} waiting for a reply");
        }
        finally
        {
            registration.Dispose();
        }
    }

    /// <summary>
    /// Sends a message to the actor and expects a response, cancelling the wait if the token trips.
    /// A message cancelled before it is batched is never delivered to the actor. Completes as cancelled
    /// with an <see cref="OperationCanceledException"/>.
    /// </summary>
    /// <param name="message"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<TResponse?> Ask(TRequest message, CancellationToken cancellationToken)
    {
        // A token that can never be cancelled needs neither a registration nor an async wrapper, so the
        // promise's own task is returned directly. Such a token is also never already-cancelled, which
        // is why the cancellation check belongs to the cancellable path alone.
        if (!cancellationToken.CanBeCanceled)
            return AskWithoutCancellation(message, null);

        return AskWithCancellation(message, null, cancellationToken);
    }

    /// <summary>
    /// Sends a message to the actor and expects a response, cancelling the wait on timeout
    /// (<see cref="AskTimeoutException"/>) or token trip (<see cref="OperationCanceledException"/>).
    /// </summary>
    /// <param name="message"></param>
    /// <param name="timeout"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="AskTimeoutException"></exception>
    public async Task<TResponse?> Ask(TRequest message, TimeSpan timeout, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        TaskCompletionSource<TResponse?> promise = Runner.SendAndTryDeliver(message, null, null);

        using CancellationTokenSource timeoutCancellationTokenSource = new(timeout);

        // A token that can never be cancelled contributes nothing to a linked source, so the timeout
        // source alone drives the wait and no linked source is created.
        using CancellationTokenSource? linkedCancellationTokenSource = cancellationToken.CanBeCanceled
            ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCancellationTokenSource.Token)
            : null;

        CancellationToken waitCancellationToken = linkedCancellationTokenSource?.Token ?? timeoutCancellationTokenSource.Token;

        CancellationTokenRegistration registration = waitCancellationToken.Register(
            static (state, token) => ((TaskCompletionSource<TResponse?>)state!).TrySetCanceled(token),
            promise
        );

        try
        {
            return await promise.Task;
        }
        catch (OperationCanceledException ex) when (ex.CancellationToken == waitCancellationToken && timeoutCancellationTokenSource.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            throw new AskTimeoutException($"Timeout after {timeout} waiting for a reply");
        }
        finally
        {
            registration.Dispose();
        }
    }

    /// <summary>
    /// Sends a message to the actor expecting a response, specifying the sender, cancelling the wait if the
    /// token trips. Completes as cancelled with an <see cref="OperationCanceledException"/>.
    /// </summary>
    /// <param name="message"></param>
    /// <param name="sender"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<TResponse?> Ask(TRequest message, IGenericActorRef sender, CancellationToken cancellationToken)
    {
        // A token that can never be cancelled needs neither a registration nor an async wrapper, so the
        // promise's own task is returned directly. Such a token is also never already-cancelled, which
        // is why the cancellation check belongs to the cancellable path alone.
        if (!cancellationToken.CanBeCanceled)
            return AskWithoutCancellation(message, sender);

        return AskWithCancellation(message, sender, cancellationToken);
    }

    /// <summary>
    /// Ask on a token that cannot be cancelled. It returns the promise's own task, so the call costs
    /// neither a cancellation registration nor a suspended async wrapper.
    /// </summary>
    /// <param name="message"></param>
    /// <param name="sender"></param>
    /// <returns></returns>
    private Task<TResponse?> AskWithoutCancellation(TRequest message, IGenericActorRef? sender)
    {
        try
        {
            return Runner.SendAndTryDeliver(message, sender, null).Task;
        }
        catch (Exception exception)
        {
            // The async overload reported a synchronous failure through the returned task, and it
            // reported an OperationCanceledException as a cancelled task instead of a faulted one.
            // The builder reproduces both outcomes exactly.
            AsyncTaskMethodBuilder<TResponse?> builder = AsyncTaskMethodBuilder<TResponse?>.Create();
            builder.SetException(exception);
            return builder.Task;
        }
    }

    /// <summary>
    /// Ask on a token that can be cancelled. If the token trips before the actor starts to process the
    /// message, the message is skipped and never delivered; if it trips while the handler already runs,
    /// the handler completes but the returned task is still cancelled.
    /// </summary>
    /// <param name="message"></param>
    /// <param name="sender"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    private async Task<TResponse?> AskWithCancellation(TRequest message, IGenericActorRef? sender, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        TaskCompletionSource<TResponse?> promise = Runner.SendAndTryDeliver(message, sender, null);

        CancellationTokenRegistration registration = cancellationToken.Register(
            static (state, token) => ((TaskCompletionSource<TResponse?>)state!).TrySetCanceled(token),
            promise
        );

        try
        {
            return await promise.Task;
        }
        finally
        {
            registration.Dispose();
        }
    }
}
