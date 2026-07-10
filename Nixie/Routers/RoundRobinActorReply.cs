
namespace Nixie.Routers;

/// <summary>
/// The router cycles through the group of routees, ensuring that for every n messages sent,
/// each actor receives one message.
/// 
/// The round robin method offers equitable distribution, with each routee receiving an equal number of messages
/// when the group remains relatively constant. However, if there are frequent changes in the group of routees,
/// the distribution might not be as balanced.
/// </summary>
/// <typeparam name="TActor"></typeparam>
/// <typeparam name="TRequest"></typeparam>
/// <typeparam name="TResponse"></typeparam>
public class RoundRobinActor<TActor, TRequest, TResponse> : IActor<TRequest, TResponse>
    where TActor : IActor<TRequest, TResponse> where TRequest : class where TResponse : class?
{
    private int position = -1;

    private readonly IActorContext<RoundRobinActor<TActor, TRequest, TResponse>, TRequest, TResponse> context;

    private readonly List<IActorRef<TActor, TRequest, TResponse>> instances = new();

    // The real reply is forwarded to a routee (ByPassReply); this completed task is only returned to satisfy
    // the signature. Cached per closed router type so Receive does not allocate a Task.FromResult per message.
    private static readonly Task<TResponse?> BypassResult = Task.FromResult<TResponse?>(default);

    /// <summary>
    /// Returns the current list of instances
    /// </summary>
    public List<IActorRef<TActor, TRequest, TResponse>> Instances => instances;

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="context"></param>
    /// <param name="numberInstances"></param>
    public RoundRobinActor(IActorContext<RoundRobinActor<TActor, TRequest, TResponse>, TRequest, TResponse> context, int numberInstances)
    {
        this.context = context;

        for (int i = 0; i < numberInstances; i++)
            instances.Add(context.ActorSystem.Spawn<TActor, TRequest, TResponse>());
    }

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="context"></param>
    /// <param name="instances"></param>
    public RoundRobinActor(IActorContext<RoundRobinActor<TActor, TRequest, TResponse>, TRequest, TResponse> context, List<IActorRef<TActor, TRequest, TResponse>> instances)
    {
        this.context = context;
        this.instances = instances;
    }

    /// <summary>
    /// Receives a message that must be routed to one of the routees
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public Task<TResponse?> Receive(TRequest message)
    {
        if ((position + 1) >= int.MaxValue)
            position = 0;
        else
            position++;

        IActorRef<TActor, TRequest, TResponse> instance = instances[position % instances.Count];
        context.ByPassReply = true; // Marks the response to be bypassed so other actor can reply
        instance.Send(message, context.Reply);

        return BypassResult;
    }
}
