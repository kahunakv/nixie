
namespace Nixie.Routers;

/// <summary>
/// Utilizes consistent hashing to choose a routee based on the transmitted message.
/// </summary>
/// <typeparam name="TActor"></typeparam>
/// <typeparam name="TRequest"></typeparam>
public class ConsistentHashActorStruct<TActor, TRequest, TResponse> : IActorStruct<TRequest, TResponse>
    where TActor : IActorStruct<TRequest, TResponse> where TRequest : struct, IConsistentHashable where TResponse : struct
{
    private readonly IActorContextStruct<ConsistentHashActorStruct<TActor, TRequest, TResponse>, TRequest, TResponse> context;

    private readonly List<IActorRefStruct<TActor, TRequest, TResponse>> instances = new();

    // The real reply is forwarded to a routee (ByPassReply); this completed task is only returned to satisfy
    // the signature. Cached per closed router type so Receive does not allocate a Task.FromResult per message.
    private static readonly Task<TResponse> BypassResult = Task.FromResult<TResponse>(default);

    /// <summary>
    /// Returns the list of instances
    /// </summary>
    public List<IActorRefStruct<TActor, TRequest, TResponse>> Instances => instances;

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="context"></param>
    /// <param name="numberInstances"></param>
    public ConsistentHashActorStruct(IActorContextStruct<ConsistentHashActorStruct<TActor, TRequest, TResponse>, TRequest, TResponse> context, int numberInstances)
    {
        this.context = context;

        instances.Capacity = numberInstances;

        for (int i = 0; i < numberInstances; i++)
            instances.Add(context.ActorSystem.SpawnStruct<TActor, TRequest, TResponse>());
    }

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="context"></param>
    /// <param name="instances"></param>
    public ConsistentHashActorStruct(IActorContextStruct<ConsistentHashActorStruct<TActor, TRequest, TResponse>, TRequest, TResponse> context, List<IActorRefStruct<TActor, TRequest, TResponse>> instances)
    {
        this.context = context;
        this.instances = instances;
    }

    /// <summary>
    /// Receives a message that must be routed to one of the routees
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public Task<TResponse> Receive(TRequest message)
    {
        int bucket = (message.GetHash() & int.MaxValue) % instances.Count;
        IActorRefStruct<TActor, TRequest, TResponse> instance = instances[bucket];
        context.ByPassReply = true; // Marks the response to be bypassed so other actor can reply
        instance.Send(message, context.Reply);
        return BypassResult;
    }
}
