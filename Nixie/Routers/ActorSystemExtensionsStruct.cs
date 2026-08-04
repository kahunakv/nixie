
namespace Nixie.Routers;

public static class ActorSystemExtensionsStruct
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
    public static IActorRefStruct<RoundRobinActorStruct<TActor, TRequest>, TRequest> CreateRoundRobinRouterStruct<TActor, TRequest>(
        this ActorSystem actorSystem,
        string name,
        int instances
    )
        where TActor : IActorStruct<TRequest> where TRequest : struct
    {        
         return actorSystem.SpawnStruct<RoundRobinActorStruct<TActor, TRequest>, TRequest>(name, instances);
    }

    /// <summary>
    /// Creates a Round-Robin router specifying number of instances
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <param name="actorSystem"></param>
    /// <param name="instances"></param>
    /// <returns></returns>
    public static IActorRefStruct<RoundRobinActorStruct<TActor, TRequest>, TRequest> CreateRoundRobinRouterStruct<TActor, TRequest>(
        this ActorSystem actorSystem,        
        int instances
    )
        where TActor : IActorStruct<TRequest> where TRequest : struct
    {
        return actorSystem.SpawnStruct<RoundRobinActorStruct<TActor, TRequest>, TRequest>(null, instances);
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
    public static IActorRefStruct<RoundRobinActorStruct<TActor, TRequest>, TRequest> CreateRoundRobinRouterStruct<TActor, TRequest>(
        this ActorSystem actorSystem,
        string name,
        List<IActorRefStruct<TActor, TRequest>> instances
    )
        where TActor : IActorStruct<TRequest> where TRequest : struct
    {
        return actorSystem.SpawnStruct<RoundRobinActorStruct<TActor, TRequest>, TRequest>(name, instances);
    }

    /// <summary>
    /// Creates a Round-Robin router specifying a list of routee actors7uh
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <param name="actorSystem"></param>
    /// <param name="instances"></param>
    /// <returns></returns>
    public static IActorRefStruct<RoundRobinActorStruct<TActor, TRequest>, TRequest> CreateRoundRobinRouterStruct<TActor, TRequest>(
        this ActorSystem actorSystem,
        List<IActorRefStruct<TActor, TRequest>> instances
    )
        where TActor : IActorStruct<TRequest> where TRequest : struct
    {
        return actorSystem.SpawnStruct<RoundRobinActorStruct<TActor, TRequest>, TRequest>(null, instances);
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
    public static IActorRefStruct<RoundRobinActorStruct<TActor, TRequest, TResponse>, TRequest, TResponse>
        CreateRoundRobinRouterStruct<TActor, TRequest, TResponse>(
            this ActorSystem actorSystem,
            string name,
            int instances
        )
        where TActor : IActorStruct<TRequest, TResponse> where TRequest : struct where TResponse : struct
    {
        return actorSystem.SpawnStruct<RoundRobinActorStruct<TActor, TRequest, TResponse>, TRequest, TResponse>(name, instances);
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
    public static IActorRefStruct<RoundRobinActorStruct<TActor, TRequest, TResponse>, TRequest, TResponse>
        CreateRoundRobinRouterStruct<TActor, TRequest, TResponse>(
            this ActorSystem actorSystem,
            int instances
        )
        where TActor : IActorStruct<TRequest, TResponse> where TRequest : struct where TResponse : struct
    {
        return actorSystem.SpawnStruct<RoundRobinActorStruct<TActor, TRequest, TResponse>, TRequest, TResponse>(null, instances);
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
    public static IActorRefStruct<RoundRobinActorStruct<TActor, TRequest, TResponse>, TRequest, TResponse>
        CreateRoundRobinRouterStruct<TActor, TRequest, TResponse>(
            this ActorSystem actorSystem,
            string name,
            List<IActorRefStruct<TActor, TRequest, TResponse>> instances
        )
        where TActor : IActorStruct<TRequest, TResponse> where TRequest : struct where TResponse : struct
    {
        return actorSystem.SpawnStruct<RoundRobinActorStruct<TActor, TRequest, TResponse>, TRequest, TResponse>(name, instances);
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
    public static IActorRefStruct<RoundRobinActorStruct<TActor, TRequest, TResponse>, TRequest, TResponse>
        CreateRoundRobinRouterStruct<TActor, TRequest, TResponse>(
            this ActorSystem actorSystem,
            List<IActorRefStruct<TActor, TRequest, TResponse>> instances
        )
        where TActor : IActorStruct<TRequest, TResponse> where TRequest : struct where TResponse : struct
    {
        return actorSystem.SpawnStruct<RoundRobinActorStruct<TActor, TRequest, TResponse>, TRequest, TResponse>(null, instances);
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
    public static IActorRefStruct<ConsistentHashActorStruct<TActor, TRequest>, TRequest> CreateConsistentHashRouterStruct<TActor, TRequest>(
        this ActorSystem actorSystem,
        string name,
        int instances
    )
        where TActor : IActorStruct<TRequest> where TRequest : struct, IConsistentHashable
    {
        return actorSystem.SpawnStruct<ConsistentHashActorStruct<TActor, TRequest>, TRequest>(name, instances);
    }
    
    /// <summary>
    /// Creates a Consistent Hash router specifying name and number of instances
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <param name="actorSystem"></param>
    /// <param name="instances"></param>
    /// <returns></returns>
    public static IActorRefStruct<ConsistentHashActorStruct<TActor, TRequest>, TRequest> CreateConsistentHashRouterStruct<TActor, TRequest>(
        this ActorSystem actorSystem,
        int instances
    )
        where TActor : IActorStruct<TRequest> where TRequest : struct, IConsistentHashable
    {
        return actorSystem.SpawnStruct<ConsistentHashActorStruct<TActor, TRequest>, TRequest>(null, instances);
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
    public static IActorRefStruct<ConsistentHashActorStruct<TActor, TRequest, TResponse>, TRequest, TResponse>
        CreateConsistentHashRouterStruct<TActor, TRequest, TResponse>(
            this ActorSystem actorSystem,
            int instances
        )
        where TActor : IActorStruct<TRequest, TResponse> where TRequest : struct, IConsistentHashable where TResponse : struct
    {
        return actorSystem.SpawnStruct<ConsistentHashActorStruct<TActor, TRequest, TResponse>, TRequest, TResponse>(null, instances);
    }
    
     /// <summary>
    /// Creates a Consistent Hash router specifying name and a list of instances
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <param name="actorSystem"></param>
    /// <param name="instances"></param>
    /// <returns></returns>
    public static IActorRefStruct<ConsistentHashActorStruct<TActor, TRequest>, TRequest> CreateConsistentHashRouterStruct<TActor, TRequest>(
        this ActorSystem actorSystem,
        List<IActorRefStruct<TActor, TRequest>> instances
    )
        where TActor : IActorStruct<TRequest> where TRequest : struct, IConsistentHashable
    {
        return actorSystem.SpawnStruct<ConsistentHashActorStruct<TActor, TRequest>, TRequest>(null, instances);
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
    public static IActorRefStruct<ConsistentHashActorStruct<TActor, TRequest>, TRequest>
        CreateConsistentHashRouterStruct<TActor, TRequest>(
            this ActorSystem actorSystem,
            string name,
            List<IActorRefStruct<TActor, TRequest>> instances
        )
        where TActor : IActorStruct<TRequest> where TRequest : struct, IConsistentHashable
    {
        return actorSystem.SpawnStruct<ConsistentHashActorStruct<TActor, TRequest>, TRequest>(name, instances);
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
    public static IActorRefStruct<ConsistentHashActorStruct<TActor, TRequest, TResponse>, TRequest, TResponse>
        CreateConsistentHashRouterStruct<TActor, TRequest, TResponse>(
            this ActorSystem actorSystem,
            string name,
            int instances
        )
        where TActor : IActorStruct<TRequest, TResponse> where TRequest : struct, IConsistentHashable where TResponse : struct
    {
        return actorSystem.SpawnStruct<ConsistentHashActorStruct<TActor, TRequest, TResponse>, TRequest, TResponse>(name, instances);
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
    public static IActorRefStruct<ConsistentHashActorStruct<TActor, TRequest, TResponse>, TRequest, TResponse>
        CreateConsistentHashRouterStruct<TActor, TRequest, TResponse>(
            this ActorSystem actorSystem,
            string name,
            List<IActorRefStruct<TActor, TRequest, TResponse>> instances
        )
        where TActor : IActorStruct<TRequest, TResponse> where TRequest : struct, IConsistentHashable where TResponse : struct
    {
        return actorSystem.SpawnStruct<ConsistentHashActorStruct<TActor, TRequest, TResponse>, TRequest, TResponse>(name, instances);
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
    public static IActorRefStruct<ConsistentHashActorStruct<TActor, TRequest, TResponse>, TRequest, TResponse>
        CreateConsistentHashRouterStruct<TActor, TRequest, TResponse>(
            this ActorSystem actorSystem,
            List<IActorRefStruct<TActor, TRequest, TResponse>> instances
        )
        where TActor : IActorStruct<TRequest, TResponse> where TRequest : struct, IConsistentHashable where TResponse : struct
    {
        return actorSystem.SpawnStruct<ConsistentHashActorStruct<TActor, TRequest, TResponse>, TRequest, TResponse>(null, instances);
    }
}