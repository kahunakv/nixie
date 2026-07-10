
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
    /// Passes a message to the actor without expecting a response and without specifying a sender
    /// </summary>
    /// <param name="message"></param>
    public void Send(TRequest message)
    {
        Runner.SendAndTryDeliver(message, null, null);
    }

    /// <summary>
    /// Passes a message to the actor without expecting a response and specifying a sender
    /// </summary>
    /// <param name="message"></param>
    /// <param name="sender"></param>
    public void Send(TRequest message, IGenericActorRef sender)
    {
        Runner.SendAndTryDeliver(message, sender, null);
    }

    /// <summary>
    /// Passes a message to the actor without expecting a response and specifying a parent promise
    /// </summary>
    /// <param name="message"></param>
    /// <param name="parentPromise"></param>
    public void Send(TRequest message, ActorMessageReply<TRequest, TResponse>? parentPromise)
    {
        Runner.SendAndTryDeliver(message, null, parentPromise);
    }

    /// <summary>
    /// Sends a message to actor expecting a response and without specifying a sender
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>    
    public async Task<TResponse?> Ask(TRequest message)
    {
        TaskCompletionSource<TResponse?> promise = Runner.SendAndTryDeliver(message, null, null);

        return await promise.Task;
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
    public async Task<TResponse?> Ask(TRequest message, IGenericActorRef sender)
    {
        TaskCompletionSource<TResponse?> promise = Runner.SendAndTryDeliver(message, sender, null);

        return await promise.Task;
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
    /// A message cancelled before it is batched is never delivered to the actor. Completes as cancelled
    /// with an <see cref="OperationCanceledException"/>.
    /// </summary>
    /// <param name="message"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<TResponse?> Ask(TRequest message, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        TaskCompletionSource<TResponse?> promise = Runner.SendAndTryDeliver(message, null, null);

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

        TaskCompletionSource<TResponse?> promise = Runner.SendAndTryDeliver(message, sender, null);

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