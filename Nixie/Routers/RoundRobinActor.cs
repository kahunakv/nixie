
// ReSharper disable ConvertToAutoPropertyWhenPossible
namespace Nixie.Routers;

/// <summary>
/// The router cycles through the group of routees, ensuring that for every n messages sent,
/// each actor receives one message.
/// 
/// The round robin method offers equitable distribution, with each routee receiving an equal number of messages
/// when the group remains relatively constant.However, if there are frequent changes in the group of routees,
/// the distribution might not be as balanced.
/// </summary>
/// <typeparam name="TActor"></typeparam>
/// <typeparam name="TRequest"></typeparam>
public class RoundRobinActor<TActor, TRequest> : IActor<TRequest>
    where TActor : IActor<TRequest> where TRequest : class
{
    private int position = -1;

    private readonly IActorContext<RoundRobinActor<TActor, TRequest>, TRequest> context;

    private readonly List<IActorRef<TActor, TRequest>> instances = [];

    /// <summary>
    /// Returns the current list of instances
    /// </summary>
    public List<IActorRef<TActor, TRequest>> Instances => instances;

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="context"></param>
    /// <param name="numberInstances"></param>
    public RoundRobinActor(IActorContext<RoundRobinActor<TActor, TRequest>, TRequest> context, int numberInstances)
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
    public RoundRobinActor(IActorContext<RoundRobinActor<TActor, TRequest>, TRequest> context, List<IActorRef<TActor, TRequest>> instances)
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
        if ((position + 1) >= int.MaxValue)
            position = 0;
        else
            position++;

        IActorRef<TActor, TRequest> instance = instances[position % instances.Count];        
        instance.Send(message);

        return Task.CompletedTask;
    }
}
