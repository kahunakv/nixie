
namespace Nixie;

/// <summary>
/// Represents a message sent to an actor that expects a reply.
/// </summary>
/// <typeparam name="TRequest"></typeparam>
/// <typeparam name="TResponse"></typeparam>
public readonly struct ActorMessageReply<TRequest, TResponse>
{
    /// <summary>
    /// Returns the request of the message.
    /// </summary>
    public TRequest Request { get; }

    /// <summary>
    /// The sender of the message
    /// </summary>
    public IGenericActorRef? Sender { get; }

    /// <summary>
    /// Returns the task completion source of the reply, or <c>null</c> for a promise-free fire-and-forget
    /// message admitted through <c>TrySend</c> (no reply is ever produced for such a message) and for a
    /// pooled-ask message, whose reply travels on <see cref="PooledHandle"/> instead.
    /// </summary>
    public TaskCompletionSource<TResponse?>? Promise { get; }

    /// <summary>
    /// The pooled reply handle for a message admitted through the pooled ask path, or the default
    /// (absent) handle for every other message. A handler that defers its reply captures this handle
    /// and completes it later; its TrySet methods are first-wins and stale-safe.
    /// </summary>
    public ReplyHandle<TResponse> PooledHandle { get; }

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="request"></param>
    /// <param name="sender"></param>
    /// <param name="promise"></param>
    public ActorMessageReply(TRequest request, IGenericActorRef? sender, TaskCompletionSource<TResponse?> promise)
    {
        Request = request;
        Sender = sender;
        Promise = promise;
    }

    /// <summary>
    /// Pooled-ask constructor: the reply travels on <paramref name="pooledHandle"/> and no task
    /// completion source exists for this message.
    /// </summary>
    /// <param name="request"></param>
    /// <param name="sender"></param>
    /// <param name="pooledHandle"></param>
    public ActorMessageReply(TRequest request, IGenericActorRef? sender, ReplyHandle<TResponse> pooledHandle)
    {
        Request = request;
        Sender = sender;
        Promise = null;
        PooledHandle = pooledHandle;
    }

    /// <summary>
    /// Promise-free constructor used by the fire-and-forget <c>TrySend</c> admission path. The delivery loop
    /// delivers the message but produces no reply (there is no promise to complete).
    /// </summary>
    /// <param name="request"></param>
    /// <param name="sender"></param>
    public ActorMessageReply(TRequest request, IGenericActorRef? sender)
    {
        Request = request;
        Sender = sender;
        Promise = null;
    }
}
