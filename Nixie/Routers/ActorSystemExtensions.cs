
namespace Nixie.Routers;

/// <summary>
/// Extensions to create routers in a friendly way
/// </summary>
public static class ActorSystemExtensions
{
    /// <summary>
    /// Creates a Round-Robin router specifying name and number of instances
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <param name="actorSystem"></param>
    /// <param name="name"></param>
    /// <param name="instances"></param>
    /// <returns></returns>
    public static IActorRef<RoundRobinActor<TActor, TRequest>, TRequest> CreateRoundRobinRouter<TActor, TRequest>(
        this ActorSystem actorSystem,
        string name,
        int instances
    )
        where TActor : IActor<TRequest> where TRequest : class
    {        
         return actorSystem.Spawn<RoundRobinActor<TActor, TRequest>, TRequest>(name, instances);
    }

    /// <summary>
    /// Creates a Round-Robin router specifying number of instances
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <param name="actorSystem"></param>
    /// <param name="instances"></param>
    /// <returns></returns>
    public static IActorRef<RoundRobinActor<TActor, TRequest>, TRequest> CreateRoundRobinRouter<TActor, TRequest>(
        this ActorSystem actorSystem,        
        int instances
    )
        where TActor : IActor<TRequest> where TRequest : class
    {
        return actorSystem.Spawn<RoundRobinActor<TActor, TRequest>, TRequest>(null, instances);
    }

    /// <summary>
    /// Creates a Round-Robin router specifying name and a list of routee actors
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <param name="actorSystem"></param>
    /// <param name="name"></param>
    /// <param name="instances"></param>
    /// <returns></returns>
    public static IActorRef<RoundRobinActor<TActor, TRequest>, TRequest> CreateRoundRobinRouter<TActor, TRequest>(
        this ActorSystem actorSystem,
        string name,
        List<IActorRef<TActor, TRequest>> instances
    )
        where TActor : IActor<TRequest> where TRequest : class
    {
        return actorSystem.Spawn<RoundRobinActor<TActor, TRequest>, TRequest>(name, instances);
    }

    /// <summary>
    /// Creates a Round-Robin router specifying a list of routee actors7uh
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <param name="actorSystem"></param>
    /// <param name="instances"></param>
    /// <returns></returns>
    public static IActorRef<RoundRobinActor<TActor, TRequest>, TRequest> CreateRoundRobinRouter<TActor, TRequest>(
        this ActorSystem actorSystem,
        List<IActorRef<TActor, TRequest>> instances
    )
        where TActor : IActor<TRequest> where TRequest : class
    {
        return actorSystem.Spawn<RoundRobinActor<TActor, TRequest>, TRequest>(null, instances);
    }

    /// <summary>
    /// Creates a Round-Robin router specifying name and number of instances
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    /// <param name="actorSystem"></param>
    /// <param name="name"></param>
    /// <param name="instances"></param>
    /// <returns></returns>
    public static IActorRef<RoundRobinActor<TActor, TRequest, TResponse>, TRequest, TResponse>
        CreateRoundRobinRouter<TActor, TRequest, TResponse>(
            this ActorSystem actorSystem,
            string name,
            int instances
        )
        where TActor : IActor<TRequest, TResponse> where TRequest : class where TResponse : class?
    {
        return actorSystem.Spawn<RoundRobinActor<TActor, TRequest, TResponse>, TRequest, TResponse>(name, instances);
    }

    /// <summary>
    /// Creates a Round-Robin router specifying name and number of instances
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    /// <param name="actorSystem"></param>
    /// <param name="instances"></param>
    /// <returns></returns>
    public static IActorRef<RoundRobinActor<TActor, TRequest, TResponse>, TRequest, TResponse>
        CreateRoundRobinRouter<TActor, TRequest, TResponse>(
            this ActorSystem actorSystem,
            int instances
        )
        where TActor : IActor<TRequest, TResponse> where TRequest : class where TResponse : class?
    {
        return actorSystem.Spawn<RoundRobinActor<TActor, TRequest, TResponse>, TRequest, TResponse>(null, instances);
    }

    /// <summary>
    /// Creates a Round-Robin router specifying name and a list of instances
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    /// <param name="actorSystem"></param>
    /// <param name="name"></param>
    /// <param name="instances"></param>
    /// <returns></returns>
    public static IActorRef<RoundRobinActor<TActor, TRequest, TResponse>, TRequest, TResponse>
        CreateRoundRobinRouter<TActor, TRequest, TResponse>(
            this ActorSystem actorSystem,
            string name,
            List<IActorRef<TActor, TRequest, TResponse>> instances
        )
        where TActor : IActor<TRequest, TResponse> where TRequest : class where TResponse : class?
    {
        return actorSystem.Spawn<RoundRobinActor<TActor, TRequest, TResponse>, TRequest, TResponse>(name, instances);
    }

    /// <summary>
    /// Creates a Round-Robin router specifying name and a list of instances
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    /// <param name="actorSystem"></param>
    /// <param name="instances"></param>
    /// <returns></returns>
    public static IActorRef<RoundRobinActor<TActor, TRequest, TResponse>, TRequest, TResponse>
        CreateRoundRobinRouter<TActor, TRequest, TResponse>(
            this ActorSystem actorSystem,
            List<IActorRef<TActor, TRequest, TResponse>> instances
        )
        where TActor : IActor<TRequest, TResponse> where TRequest : class where TResponse : class?
    {
        return actorSystem.Spawn<RoundRobinActor<TActor, TRequest, TResponse>, TRequest, TResponse>(null, instances);
    }

    /// <summary>
    /// Creates a Consistent Hash router specifying name and number of instances
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <param name="actorSystem"></param>
    /// <param name="name"></param>
    /// <param name="instances"></param>
    /// <returns></returns>
    public static IActorRef<ConsistentHashActor<TActor, TRequest>, TRequest> CreateConsistentHashRouter<TActor, TRequest>(
        this ActorSystem actorSystem,
        string name,
        int instances
    )
        where TActor : IActor<TRequest> where TRequest : class, IConsistentHashable
    {
        return actorSystem.Spawn<ConsistentHashActor<TActor, TRequest>, TRequest>(name, instances);
    }

    /// <summary>
    /// Creates a Consistent Hash router specifying name and number of instances
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <param name="actorSystem"></param>
    /// <param name="instances"></param>
    /// <returns></returns>
    public static IActorRef<ConsistentHashActor<TActor, TRequest>, TRequest> CreateConsistentHashRouter<TActor, TRequest>(
        this ActorSystem actorSystem,
        int instances
    )
        where TActor : IActor<TRequest> where TRequest : class, IConsistentHashable
    {
        return actorSystem.Spawn<ConsistentHashActor<TActor, TRequest>, TRequest>(null, instances);
    }

    /// <summary>
    /// Creates a Round-Robin router specifying a list of instances
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <param name="actorSystem"></param>
    /// <param name="name"></param>
    /// <param name="instances"></param>
    /// <returns></returns>
    public static IActorRef<ConsistentHashActor<TActor, TRequest>, TRequest> CreateConsistentHashRouter<TActor, TRequest>(
        this ActorSystem actorSystem,
        string name,
        List<IActorRef<TActor, TRequest>> instances
    )
        where TActor : IActor<TRequest> where TRequest : class, IConsistentHashable
    {
        return actorSystem.Spawn<ConsistentHashActor<TActor, TRequest>, TRequest>(name, instances);
    }

    /// <summary>
    /// Creates a Consistent Hash router specifying name and a list of instances
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <param name="actorSystem"></param>
    /// <param name="instances"></param>
    /// <returns></returns>
    public static IActorRef<ConsistentHashActor<TActor, TRequest>, TRequest> CreateConsistentHashRouter<TActor, TRequest>(
        this ActorSystem actorSystem,
        List<IActorRef<TActor, TRequest>> instances
    )
        where TActor : IActor<TRequest> where TRequest : class, IConsistentHashable
    {
        return actorSystem.Spawn<ConsistentHashActor<TActor, TRequest>, TRequest>(null, instances);
    }

    /// <summary>
    /// Creates a Consistent Hash router specifying name and number of instances
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    /// <param name="actorSystem"></param>
    /// <param name="name"></param>
    /// <param name="instances"></param>
    /// <returns></returns>
    public static IActorRef<ConsistentHashActor<TActor, TRequest, TResponse>, TRequest, TResponse>
        CreateConsistentHashRouter<TActor, TRequest, TResponse>(
            this ActorSystem actorSystem,
            string name,
            int instances
        )
        where TActor : IActor<TRequest, TResponse> where TRequest : class, IConsistentHashable where TResponse : class?
    {
        return actorSystem.Spawn<ConsistentHashActor<TActor, TRequest, TResponse>, TRequest, TResponse>(name, instances);
    }

    /// <summary>
    /// Creates a Consistent Hash router specifying name and number of instances
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    /// <param name="actorSystem"></param>
    /// <param name="instances"></param>
    /// <returns></returns>
    public static IActorRef<ConsistentHashActor<TActor, TRequest, TResponse>, TRequest, TResponse>
        CreateConsistentHashRouter<TActor, TRequest, TResponse>(
            this ActorSystem actorSystem,
            int instances
        )
        where TActor : IActor<TRequest, TResponse> where TRequest : class, IConsistentHashable where TResponse : class?
    {
        return actorSystem.Spawn<ConsistentHashActor<TActor, TRequest, TResponse>, TRequest, TResponse>(null, instances);
    }

    /// <summary>
    /// Creates a Consistent Hash router specifying name and a list of instances
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    /// <param name="actorSystem"></param>
    /// <param name="name"></param>
    /// <param name="instances"></param>
    /// <returns></returns>
    public static IActorRef<ConsistentHashActor<TActor, TRequest, TResponse>, TRequest, TResponse>
        CreateConsistentHashRouter<TActor, TRequest, TResponse>(
            this ActorSystem actorSystem,
            string name,
            List<IActorRef<TActor, TRequest, TResponse>> instances
        )
        where TActor : IActor<TRequest, TResponse> where TRequest : class, IConsistentHashable where TResponse : class?
    {
        return actorSystem.Spawn<ConsistentHashActor<TActor, TRequest, TResponse>, TRequest, TResponse>(name, instances);
    }

    /// <summary>
    /// Creates a Consistent Hash router specifying name and a list of instances
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    /// <param name="actorSystem"></param>
    /// <param name="instances"></param>
    /// <returns></returns>
    public static IActorRef<ConsistentHashActor<TActor, TRequest, TResponse>, TRequest, TResponse>
        CreateConsistentHashRouter<TActor, TRequest, TResponse>(
            this ActorSystem actorSystem,
            List<IActorRef<TActor, TRequest, TResponse>> instances
        )
        where TActor : IActor<TRequest, TResponse> where TRequest : class, IConsistentHashable where TResponse : class?
    {
        return actorSystem.Spawn<ConsistentHashActor<TActor, TRequest, TResponse>, TRequest, TResponse>(null, instances);
    }

    /// <summary>
    /// Creates a Balancing router specifying name and number of instances
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <param name="actorSystem"></param>
    /// <param name="name"></param>
    /// <param name="instances"></param>
    /// <returns></returns>
    public static IActorRef<BalancingActor<TActor, TRequest>, TRequest> CreateBalancingRouter<TActor, TRequest>(
        this ActorSystem actorSystem,
        string name,
        int instances
    )
        where TActor : IActor<TRequest> where TRequest : class
    {
        return actorSystem.Spawn<BalancingActor<TActor, TRequest>, TRequest>(name, instances);
    }

    /// <summary>
    /// Creates a Balancing router specifying number of instances
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <param name="actorSystem"></param>
    /// <param name="instances"></param>
    /// <returns></returns>
    public static IActorRef<BalancingActor<TActor, TRequest>, TRequest> CreateBalancingRouter<TActor, TRequest>(
        this ActorSystem actorSystem,
        int instances
    )
        where TActor : IActor<TRequest> where TRequest : class
    {
        return actorSystem.Spawn<BalancingActor<TActor, TRequest>, TRequest>(null, instances);
    }

    /// <summary>
    /// Creates a Balancing router specifying name and a list of routee actors
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <param name="actorSystem"></param>
    /// <param name="name"></param>
    /// <param name="instances"></param>
    /// <returns></returns>
    public static IActorRef<BalancingActor<TActor, TRequest>, TRequest> CreateBalancingRouter<TActor, TRequest>(
        this ActorSystem actorSystem,
        string name,
        List<IActorRef<TActor, TRequest>> instances
    )
        where TActor : IActor<TRequest> where TRequest : class
    {
        return actorSystem.Spawn<BalancingActor<TActor, TRequest>, TRequest>(name, instances);
    }

    /// <summary>
    /// Creates a Balancing router specifying a list of routee actors
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <param name="actorSystem"></param>
    /// <param name="instances"></param>
    /// <returns></returns>
    public static IActorRef<BalancingActor<TActor, TRequest>, TRequest> CreateBalancingRouter<TActor, TRequest>(
        this ActorSystem actorSystem,
        List<IActorRef<TActor, TRequest>> instances
    )
        where TActor : IActor<TRequest> where TRequest : class
    {
        return actorSystem.Spawn<BalancingActor<TActor, TRequest>, TRequest>(null, instances);
    }
}
