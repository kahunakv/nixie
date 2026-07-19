
namespace Nixie;

/// <summary>
/// Represents an actor reference.
/// </summary>
/// <typeparam name="TActor"></typeparam>
/// <typeparam name="TRequest"></typeparam>
/// <typeparam name="TResponse"></typeparam>
public sealed class ActorRef<TActor, TRequest, TResponse> : IGenericActorRef, IActorRef<TActor, TRequest, TResponse>
    where TActor : IActor<TRequest, TResponse> where TRequest : class where TResponse : class?
{
    private readonly ActorRunner<TActor, TRequest, TResponse> runner;

    /// <summary>
    /// Returns a reference to the actor's runner
    /// </summary>
    public ActorRunner<TActor, TRequest, TResponse> Runner => runner;

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="runner"></param>
    public ActorRef(ActorRunner<TActor, TRequest, TResponse> runner)
    {
        this.runner = runner;
    }

    /// <summary>
    /// Passes a message to the actor without expecting a response and without specifying a sender
    /// </summary>
    /// <param name="message"></param>
    public void Send(TRequest message)
    {
        runner.SendAndTryDeliver(message, null, null);
    }

    /// <summary>
    /// Passes a message to the actor without expecting a response and specifying a sender
    /// </summary>
    /// <param name="message"></param>
    /// <param name="sender"></param>
    public void Send(TRequest message, IGenericActorRef sender)
    {
        runner.SendAndTryDeliver(message, sender, null);
    }

    /// <summary>
    /// Passes a message to the actor without expecting a response and specifying a parent promise
    /// </summary>
    /// <param name="message"></param>
    /// <param name="parentPromise"></param>
    public void Send(TRequest message, ActorMessageReply<TRequest, TResponse>? parentPromise)
    {
        runner.SendAndTryDeliver(message, null, parentPromise);
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
        return runner.TrySend(message, null);
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
        return runner.TrySend(message, sender);
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
            return runner.SendAndTryDeliver(message, null, null).Task;
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
        TaskCompletionSource<TResponse?> promise = runner.SendAndTryDeliver(message, null, null);

        using CancellationTokenSource timeoutCancellationTokenSource = new(timeout);

        CancellationTokenRegistration registration = timeoutCancellationTokenSource.Token.Register(
            static state => ((TaskCompletionSource<TResponse?>)state!).TrySetCanceled(),
            promise
        );

        try
        {
            return await promise.Task;
        }
        catch (OperationCanceledException) when (timeoutCancellationTokenSource.IsCancellationRequested)
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
            return runner.SendAndTryDeliver(message, sender, null).Task;
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
        TaskCompletionSource<TResponse?> promise = runner.SendAndTryDeliver(message, sender, null);

        using CancellationTokenSource timeoutCancellationTokenSource = new(timeout);

        CancellationTokenRegistration registration = timeoutCancellationTokenSource.Token.Register(
            static state => ((TaskCompletionSource<TResponse?>)state!).TrySetCanceled(),
            promise
        );

        try
        {
            return await promise.Task;
        }
        catch (OperationCanceledException) when (timeoutCancellationTokenSource.IsCancellationRequested)
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
    /// If the token trips before the actor has started processing the message, the message is skipped
    /// and never delivered; if it trips while the handler is already running, the handler completes but
    /// the returned task is still cancelled. Completes as cancelled with an <see cref="OperationCanceledException"/>.
    /// </summary>
    /// <param name="message"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<TResponse?> Ask(TRequest message, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        TaskCompletionSource<TResponse?> promise = runner.SendAndTryDeliver(message, null, null);

        CancellationTokenRegistration registration = cancellationToken.Register(
            static state => ((TaskCompletionSource<TResponse?>)state!).TrySetCanceled(),
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

    /// <summary>
    /// Sends a message to the actor and expects a response, cancelling the wait when either the timeout
    /// elapses (surfacing <see cref="AskTimeoutException"/>) or the token trips (surfacing
    /// <see cref="OperationCanceledException"/>).
    /// </summary>
    /// <param name="message"></param>
    /// <param name="timeout"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="AskTimeoutException"></exception>
    public async Task<TResponse?> Ask(TRequest message, TimeSpan timeout, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        TaskCompletionSource<TResponse?> promise = runner.SendAndTryDeliver(message, null, null);

        using CancellationTokenSource timeoutCancellationTokenSource = new(timeout);
        using CancellationTokenSource linkedCancellationTokenSource =
            CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCancellationTokenSource.Token);

        CancellationTokenRegistration registration = linkedCancellationTokenSource.Token.Register(
            static state => ((TaskCompletionSource<TResponse?>)state!).TrySetCanceled(),
            promise
        );

        try
        {
            return await promise.Task;
        }
        catch (OperationCanceledException) when (timeoutCancellationTokenSource.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
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
    public async Task<TResponse?> Ask(TRequest message, IGenericActorRef sender, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        TaskCompletionSource<TResponse?> promise = runner.SendAndTryDeliver(message, sender, null);

        CancellationTokenRegistration registration = cancellationToken.Register(
            static state => ((TaskCompletionSource<TResponse?>)state!).TrySetCanceled(),
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