
namespace Nixie.Routers;

/// <summary>
/// Utilizes consistent hashing to choose a routee based on the transmitted message.
/// </summary>
/// <typeparam name="TActor"></typeparam>
/// <typeparam name="TRequest"></typeparam>
public class ConsistentHashActorStruct<TActor, TRequest> : IActorStruct<TRequest>
    where TActor : IActorStruct<TRequest> where TRequest : struct, IConsistentHashable
{
    private readonly IActorContextStruct<ConsistentHashActorStruct<TActor, TRequest>, TRequest> context;

    private readonly List<IActorRefStruct<TActor, TRequest>> instances = new();

    /// <summary>
    /// Returns the list of instances
    /// </summary>
    public List<IActorRefStruct<TActor, TRequest>> Instances => instances;

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="context"></param>
    /// <param name="numberInstances"></param>
    public ConsistentHashActorStruct(IActorContextStruct<ConsistentHashActorStruct<TActor, TRequest>, TRequest> context, int numberInstances)
    {
        this.context = context;

        for (int i = 0; i < numberInstances; i++)
            instances.Add(context.ActorSystem.SpawnStruct<TActor, TRequest>());
    }

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="context"></param>
    /// <param name="instances"></param>
    public ConsistentHashActorStruct(IActorContextStruct<ConsistentHashActorStruct<TActor, TRequest>, TRequest> context, List<IActorRefStruct<TActor, TRequest>> instances)
    {
        this.context = context;
        this.instances = instances;
    }

    /// <summary>
    /// Receives a message that must be routed to one of the routees
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public Task Receive(TRequest message)
    {
        IActorRefStruct<TActor, TRequest> instance = instances[(message.GetHash() & int.MaxValue) % instances.Count];
        instance.Send(message);

        return Task.CompletedTask;
    }
}