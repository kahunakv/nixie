
// ReSharper disable ConvertToAutoPropertyWhenPossible
namespace Nixie.Routers;

/// <summary>
/// Utilizes consistent hashing to choose a routee based on the transmitted message.
/// </summary>
/// <typeparam name="TActor"></typeparam>
/// <typeparam name="TRequest"></typeparam>
public class ConsistentHashActor<TActor, TRequest> : IActor<TRequest>
    where TActor : IActor<TRequest> where TRequest : class, IConsistentHashable
{
    private readonly IActorContext<ConsistentHashActor<TActor, TRequest>, TRequest> context;

    private readonly List<IActorRef<TActor, TRequest>> instances = new();

    /// <summary>
    /// Returns the list of instances
    /// </summary>
    public List<IActorRef<TActor, TRequest>> Instances => instances;

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="context"></param>
    /// <param name="numberInstances"></param>
    public ConsistentHashActor(IActorContext<ConsistentHashActor<TActor, TRequest>, TRequest> context, int numberInstances)
    {
        this.context = context;

        instances.Capacity = numberInstances;

        for (int i = 0; i < numberInstances; i++)
            instances.Add(context.ActorSystem.Spawn<TActor, TRequest>());
    }

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="context"></param>
    /// <param name="instances"></param>
    public ConsistentHashActor(IActorContext<ConsistentHashActor<TActor, TRequest>, TRequest> context, List<IActorRef<TActor, TRequest>> instances)
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
        IActorRef<TActor, TRequest> instance = instances[(message.GetHash() & int.MaxValue) % instances.Count];
        instance.Send(message);

        return Task.CompletedTask;
    }
}
