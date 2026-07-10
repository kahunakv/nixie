
namespace Nixie;

/// <summary>
/// Represents an actor reference.
/// </summary>
/// <typeparam name="TActor"></typeparam>
/// <typeparam name="TRequest"></typeparam>
/// <typeparam name="TResponse"></typeparam>
public interface IActorRefAggregate<TActor, TRequest, TResponse> 
    where TActor : IActorAggregate<TRequest, TResponse> where TRequest : class where TResponse : class?
{
    /// <summary>
    /// The actor runner.
    /// </summary>
    public ActorRunnerAggregate<TActor, TRequest, TResponse> Runner { get; }

    /// <summary>
    /// Sends a message to the actor without expecting response
    /// </summary>
    /// <param name="message"></param>
    public void Send(TRequest message);

    /// <summary>
    /// Sends a message to the actor without expecting response
    /// </summary>
    /// <param name="message"></param>
    /// <param name="message"></param>
    public void Send(TRequest message, IGenericActorRef sender);

    /// <summary>
    /// Passes a message to the actor without expecting a response and specifying a parent promise
    /// </summary>
    /// <param name="message"></param>
    /// <param name="parentPromise"></param>
    public void Send(TRequest message, ActorMessageReply<TRequest, TResponse>? parentPromise);

    /// <summary>
    /// Sends a message to the actor and expects a response
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public Task<TResponse?> Ask(TRequest message);

    /// <summary>
    /// Sends a message to the actor and expects a response
    /// An exception will be thrown if the timeout limit is reached
    /// </summary>
    /// <param name="message"></param>
    /// <param name="timeout"></param>
    /// <returns></returns>
    public Task<TResponse?> Ask(TRequest message, TimeSpan timeout);

    /// <summary>
    /// Sends a message to the actor and expects a response
    /// </summary>
    /// <param name="message"></param>
    /// <param name="sender"></param>
    /// <returns></returns>
    public Task<TResponse?> Ask(TRequest message, IGenericActorRef sender);

    /// <summary>
    /// Sends a message to the actor and expects a response
    /// An exception will be thrown if the timeout limit is reached
    /// </summary>
    /// <param name="message"></param>
    /// <param name="sender"></param>
    /// <returns></returns>
    public Task<TResponse?> Ask(TRequest message, IGenericActorRef sender, TimeSpan timeout);

    /// <summary>
    /// Sends a message to the actor and expects a response, cancelling the wait if the token trips.
    /// A message cancelled before it is batched is never delivered to the actor.
    /// </summary>
    /// <param name="message"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<TResponse?> Ask(TRequest message, CancellationToken cancellationToken);

    /// <summary>
    /// Sends a message to the actor and expects a response, cancelling the wait on timeout or token trip.
    /// </summary>
    /// <param name="message"></param>
    /// <param name="timeout"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<TResponse?> Ask(TRequest message, TimeSpan timeout, CancellationToken cancellationToken);

    /// <summary>
    /// Sends a message to the actor and expects a response, specifying the sender, cancelling the wait if
    /// the token trips.
    /// </summary>
    /// <param name="message"></param>
    /// <param name="sender"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<TResponse?> Ask(TRequest message, IGenericActorRef sender, CancellationToken cancellationToken);
}
