
using Nixie.Actors;
using Nixie.Utils;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;

namespace Nixie;

/// <summary>
/// The actor system encapsulates and encompasses all the actors and their references created. It is possible to have many independent actor systems running.
/// </summary>
public sealed class ActorSystem : IDisposable
{
    private readonly ActorScheduler scheduler;

    private readonly IServiceProvider? serviceProvider;

    private readonly ILogger? logger;

    private readonly ConcurrentDictionary<Type, Lazy<IActorRepositoryRunnable>> repositories = new();

    /// <summary>
    /// Returns the reference to the nobody actor. This actor is used when a message is sent to an actor that doesn't exist.
    /// </summary>
    public IActorRef<NobodyActor, object> Nobody { get; }

    /// <summary>
    /// Returns the actor scheduler
    /// </summary>
    public ActorScheduler Scheduler => scheduler;

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="serviceProvider"></param>
    /// <param name="logger"></param>
    public ActorSystem(IServiceProvider? serviceProvider = null, ILogger? logger = null)
    {
        this.serviceProvider = serviceProvider;
        this.logger = logger;
        this.scheduler = new(this, logger);

        Nobody = Spawn<NobodyActor, object>();
    }

    /// <summary>
    /// Creates a new request/response actor and returns a typed reference.
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    /// <param name="name"></param>
    /// <param name="args"></param>
    /// <returns></returns>
    public IActorRef<TActor, TRequest, TResponse> Spawn<TActor, TRequest, TResponse>(string? name = null, params object[]? args)
        where TActor : IActor<TRequest, TResponse> where TRequest : class where TResponse : class?
    {
        ActorRepository<TActor, TRequest, TResponse> repository = GetRepository<TActor, TRequest, TResponse>();

        return repository.Spawn(name, args);
    }

    /// <summary>
    /// Creates a new request/response actor with options (e.g. a bounded inbox) and returns a typed reference.
    /// </summary>
    public IActorRef<TActor, TRequest, TResponse> SpawnWithOptions<TActor, TRequest, TResponse>(string? name, ActorRunnerOptions? options, params object[]? args)
        where TActor : IActor<TRequest, TResponse> where TRequest : class where TResponse : class?
    {
        ActorRepository<TActor, TRequest, TResponse> repository = GetRepository<TActor, TRequest, TResponse>();

        return repository.SpawnWithOptions(name, options, args);
    }

    /// <summary>
    /// Creates a new fire-n-forget aggregate actor and returns a typed reference.
    /// Aggreate actors receive a batch of messages and process them in a single execution instead of one by one.
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <param name="name"></param>
    /// <param name="args"></param>
    /// <returns></returns>
    public IActorRefAggregate<TActor, TRequest> SpawnAggregate<TActor, TRequest>(string? name = null, params object[]? args)
        where TActor : IActorAggregate<TRequest> where TRequest : class
    {
        ActorRepositoryAggregate<TActor, TRequest> repository = GetRepositoryAggregate<TActor, TRequest>();

        return repository.Spawn(name, args);
    }

    /// <summary>
    /// Creates a new fire-n-forget aggregate actor with options (e.g. a bounded inbox) and returns a typed reference.
    /// </summary>
    public IActorRefAggregate<TActor, TRequest> SpawnAggregateWithOptions<TActor, TRequest>(string? name, ActorRunnerOptions? options, params object[]? args)
        where TActor : IActorAggregate<TRequest> where TRequest : class
    {
        ActorRepositoryAggregate<TActor, TRequest> repository = GetRepositoryAggregate<TActor, TRequest>();

        return repository.SpawnWithOptions(name, options, args);
    }

    /// <summary>
    /// Creates a new fire-n-forget actor and returns a typed reference.
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <param name="name"></param>
    /// <param name="args"></param>
    /// <returns></returns>
    public IActorRef<TActor, TRequest> Spawn<TActor, TRequest>(string? name = null, params object[]? args)
        where TActor : IActor<TRequest> where TRequest : class
    {
        ActorRepository<TActor, TRequest> repository = GetRepository<TActor, TRequest>();

        return repository.Spawn(name, args);
    }

    /// <summary>
    /// Creates a new fire-n-forget actor with options (e.g. a bounded inbox) and returns a typed reference.
    /// </summary>
    public IActorRef<TActor, TRequest> SpawnWithOptions<TActor, TRequest>(string? name, ActorRunnerOptions? options, params object[]? args)
        where TActor : IActor<TRequest> where TRequest : class
    {
        ActorRepository<TActor, TRequest> repository = GetRepository<TActor, TRequest>();

        return repository.SpawnWithOptions(name, options, args);
    }

    /// <summary>
    /// Creates a new request/response actor and returns a typed reference.
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    /// <param name="name"></param>
    /// <param name="args"></param>
    /// <returns></returns>
    public IActorRefStruct<TActor, TRequest, TResponse> SpawnStruct<TActor, TRequest, TResponse>(string? name = null, params object[]? args)
        where TActor : IActorStruct<TRequest, TResponse> where TRequest : struct where TResponse : struct
    {
        ActorRepositoryStruct<TActor, TRequest, TResponse> repository = GetRepositoryStruct<TActor, TRequest, TResponse>();

        return repository.Spawn(name, args);
    }

    /// <summary>
    /// Creates a new request/response struct actor with options (e.g. a bounded inbox) and returns a typed reference.
    /// </summary>
    public IActorRefStruct<TActor, TRequest, TResponse> SpawnStructWithOptions<TActor, TRequest, TResponse>(string? name, ActorRunnerOptions? options, params object[]? args)
        where TActor : IActorStruct<TRequest, TResponse> where TRequest : struct where TResponse : struct
    {
        ActorRepositoryStruct<TActor, TRequest, TResponse> repository = GetRepositoryStruct<TActor, TRequest, TResponse>();

        return repository.SpawnWithOptions(name, options, args);
    }

    /// <summary>
    /// Creates a new fire-n-forget actor and returns a typed reference to a struct actor
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <param name="name"></param>
    /// <param name="args"></param>
    /// <returns></returns>
    public IActorRefStruct<TActor, TRequest> SpawnStruct<TActor, TRequest>(string? name = null, params object[]? args)
        where TActor : IActorStruct<TRequest> where TRequest : struct
    {
        ActorRepositoryStruct<TActor, TRequest> repository = GetRepositoryStruct<TActor, TRequest>();

        return repository.Spawn(name, args);
    }

    /// <summary>
    /// Creates a new fire-n-forget struct actor with options (e.g. a bounded inbox) and returns a typed reference.
    /// </summary>
    public IActorRefStruct<TActor, TRequest> SpawnStructWithOptions<TActor, TRequest>(string? name, ActorRunnerOptions? options, params object[]? args)
        where TActor : IActorStruct<TRequest> where TRequest : struct
    {
        ActorRepositoryStruct<TActor, TRequest> repository = GetRepositoryStruct<TActor, TRequest>();

        return repository.SpawnWithOptions(name, options, args);
    }

    /// <summary>
    /// Creates a new request/response aggregate actor and returns a typed reference.
    /// Aggreate actors receive a batch of messages and process them in a single execution instead of one by one.
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <param name="name"></param>
    /// <param name="args"></param>
    /// <returns></returns>
    public IActorRefAggregate<TActor, TRequest, TResponse> SpawnAggregate<TActor, TRequest, TResponse>(string? name = null, params object[]? args)
        where TActor : IActorAggregate<TRequest, TResponse> where TRequest : class where TResponse : class?
    {
        ActorRepositoryAggregate<TActor, TRequest, TResponse> repository = GetRepositoryAggregate<TActor, TRequest, TResponse>();

        return repository.Spawn(name, args);
    }

    /// <summary>
    /// Creates a new request/response aggregate actor with options (e.g. a bounded inbox) and returns a typed reference.
    /// </summary>
    public IActorRefAggregate<TActor, TRequest, TResponse> SpawnAggregateWithOptions<TActor, TRequest, TResponse>(string? name, ActorRunnerOptions? options, params object[]? args)
        where TActor : IActorAggregate<TRequest, TResponse> where TRequest : class where TResponse : class?
    {
        ActorRepositoryAggregate<TActor, TRequest, TResponse> repository = GetRepositoryAggregate<TActor, TRequest, TResponse>();

        return repository.SpawnWithOptions(name, options, args);
    }

    /// <summary>
    /// Returns a request/response actor by its name and null if it doesn't exist.
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    /// <param name="name"></param>
    /// <returns></returns>
    public IActorRef<TActor, TRequest, TResponse>? Get<TActor, TRequest, TResponse>(string name)
        where TActor : IActor<TRequest, TResponse> where TRequest : class where TResponse : class
    {
        ActorRepository<TActor, TRequest, TResponse> repository = GetRepository<TActor, TRequest, TResponse>();

        return repository.Get(name);
    }

    /// <summary>
    /// Returns a fire-n-forget actor by its name and null if it doesn't exist.
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    /// <param name="name"></param>
    /// <returns></returns>
    public IActorRef<TActor, TRequest>? Get<TActor, TRequest>(string name) where TActor : IActor<TRequest>
        where TRequest : class
    {
        ActorRepository<TActor, TRequest> repository = GetRepository<TActor, TRequest>();

        return repository.Get(name);
    }

    /// <summary>
    /// Returns a fire-n-forget actor by its name and null if it doesn't exist.
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    /// <param name="name"></param>
    /// <returns></returns>
    public IActorRefStruct<TActor, TRequest>? GetStruct<TActor, TRequest>(string name) where TActor : IActorStruct<TRequest>
        where TRequest : struct
    {
        ActorRepositoryStruct<TActor, TRequest> repository = GetRepositoryStruct<TActor, TRequest>();

        return repository.Get(name);
    }

    /// <summary>
    /// Shutdowns an actor by its name
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    /// <param name="name"></param>
    /// <returns></returns>
    public bool Shutdown<TActor, TRequest, TResponse>(string name) where TActor : IActor<TRequest, TResponse>
        where TRequest : class where TResponse : class
    {
        ActorRepository<TActor, TRequest, TResponse> repository = GetRepository<TActor, TRequest, TResponse>();

        return repository.Shutdown(name);
    }    

    /// <summary>
    /// Shutdowns an actor by its reference
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    /// <param name="name"></param>
    /// <returns></returns>
    public bool Shutdown<TActor, TRequest, TResponse>(IActorRef<TActor, TRequest, TResponse> actorRef)
        where TActor : IActor<TRequest, TResponse> where TRequest : class where TResponse : class?
    {
        ActorRepository<TActor, TRequest, TResponse> repository = GetRepository<TActor, TRequest, TResponse>();

        return repository.Shutdown(actorRef);
    }

    /// <summary>
    /// Shutdowns an actor by its name
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    /// <param name="name"></param>
    /// <returns></returns>
    public bool Shutdown<TActor, TRequest>(string name) where TActor : IActor<TRequest>
        where TRequest : class
    {
        ActorRepository<TActor, TRequest> repository = GetRepository<TActor, TRequest>();

        return repository.Shutdown(name);
    }

    /// <summary>
    /// Shutdowns an actor by its reference
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    /// <param name="name"></param>
    /// <returns></returns>
    public bool Shutdown<TActor, TRequest>(IActorRef<TActor, TRequest> actorRef)
        where TActor : IActor<TRequest> where TRequest : class
    {
        ActorRepository<TActor, TRequest> repository = GetRepository<TActor, TRequest>();

        return repository.Shutdown(actorRef);
    }

    /// <summary>
    /// Shutdowns an actor by its reference
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    /// <param name="name"></param>
    /// <returns></returns>
    public bool ShutdownStruct<TActor, TRequest, TResponse>(IActorRefStruct<TActor, TRequest, TResponse> actorRef)
        where TActor : IActorStruct<TRequest, TResponse> where TRequest : struct where TResponse : struct
    {
        ActorRepositoryStruct<TActor, TRequest, TResponse> repository = GetRepositoryStruct<TActor, TRequest, TResponse>();

        return repository.Shutdown(actorRef);
    }

    /// <summary>
    /// Shutdowns an actor by its name
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    /// <param name="name"></param>
    /// <returns></returns>
    public bool ShutdownStruct<TActor, TRequest>(string name) where TActor : IActorStruct<TRequest>
        where TRequest : struct
    {
        ActorRepositoryStruct<TActor, TRequest> repository = GetRepositoryStruct<TActor, TRequest>();

        return repository.Shutdown(name);
    }    

    /// <summary>
    /// Shutdowns an actor by its reference
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    /// <param name="name"></param>
    /// <returns></returns>
    public bool ShutdownStruct<TActor, TRequest>(IActorRefStruct<TActor, TRequest> actorRef)
        where TActor : IActorStruct<TRequest> where TRequest : struct
    {
        ActorRepositoryStruct<TActor, TRequest> repository = GetRepositoryStruct<TActor, TRequest>();

        return repository.Shutdown(actorRef);
    }

    /// <summary>
    /// Tries to shutdown an actor by its name and returns a task whose result confirms shutdown within the specified timespan
    /// </summary>
    /// <param name="name"></param>
    /// <param name="maxWait"></param>
    /// <returns></returns>
    public async Task<bool> GracefulShutdown<TActor, TRequest>(string name, TimeSpan maxWait) where TActor : IActor<TRequest>
        where TRequest : class
    {
        ActorRepository<TActor, TRequest> repository = GetRepository<TActor, TRequest>();

        return await repository.GracefulShutdown(name, maxWait);
    }

    /// <summary>
    /// Tries to shutdown an actor by its name and returns a task whose result confirms shutdown within the specified timespan
    /// </summary>
    /// <param name="name"></param>
    /// <param name="maxWait"></param>
    /// <returns></returns>
    public async Task<bool> GracefulShutdown<TActor, TRequest, TResponse>(string name, TimeSpan maxWait) where TActor : IActor<TRequest, TResponse>
        where TRequest : class where TResponse : class
    {
        ActorRepository<TActor, TRequest, TResponse> repository = GetRepository<TActor, TRequest, TResponse>();

        return await repository.GracefulShutdown(name, maxWait);
    }
    
    /// <summary>
    /// Shutdowns an actor by its reference
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>    
    /// <param name="name"></param>
    /// <param name="maxWait"></param>
    /// <returns></returns>
    public async Task<bool> GracefulShutdown<TActor, TRequest>(IActorRef<TActor, TRequest> actorRef, TimeSpan maxWait)
        where TActor : IActor<TRequest> where TRequest : class
    {
        ActorRepository<TActor, TRequest> repository = GetRepository<TActor, TRequest>();

        return await repository.GracefulShutdown(actorRef, maxWait);
    }

    /// <summary>
    /// Shutdowns an actor by its reference
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    /// <param name="name"></param>
    /// <param name="maxWait"></param>
    /// <returns></returns>
    public async Task<bool> GracefulShutdown<TActor, TRequest, TResponse>(IActorRef<TActor, TRequest, TResponse> actorRef, TimeSpan maxWait)
        where TActor : IActor<TRequest, TResponse> where TRequest : class where TResponse : class
    {
        ActorRepository<TActor, TRequest, TResponse> repository = GetRepository<TActor, TRequest, TResponse>();

        return await repository.GracefulShutdown(actorRef, maxWait);
    }

    /// <summary>
    /// Tries to shutdown an actor by its name and returns a task whose result confirms shutdown within the specified timespan
    /// </summary>
    /// <param name="name"></param>
    /// <param name="maxWait"></param>
    /// <returns></returns>
    public async Task<bool> GracefulShutdownStruct<TActor, TRequest>(string name, TimeSpan maxWait) where TActor : IActorStruct<TRequest>
        where TRequest : struct
    {
        ActorRepositoryStruct<TActor, TRequest> repository = GetRepositoryStruct<TActor, TRequest>();

        return await repository.GracefulShutdown(name, maxWait);
    }

    /// <summary>
    /// Shutdowns an actor by its reference
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    /// <param name="actorRef"></param>
    /// <param name="maxWait"></param>
    /// <returns></returns>
    public async Task<bool> GracefulShutdownStruct<TActor, TRequest, TResponse>(IActorRefStruct<TActor, TRequest, TResponse> actorRef, TimeSpan maxWait)
        where TActor : IActorStruct<TRequest, TResponse> where TRequest : struct where TResponse : struct
    {
        ActorRepositoryStruct<TActor, TRequest, TResponse> repository = GetRepositoryStruct<TActor, TRequest, TResponse>();

        return await repository.GracefulShutdown(actorRef, maxWait);
    }

    /// <summary>
    /// Shutdowns an actor by its reference
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>    
    /// <param name="actorRef"></param>
    /// <param name="maxWait"></param>
    /// <returns></returns>
    public async Task<bool> GracefulShutdownStruct<TActor, TRequest>(IActorRefStruct<TActor, TRequest> actorRef, TimeSpan maxWait)
        where TActor : IActorStruct<TRequest> where TRequest : struct 
    {
        ActorRepositoryStruct<TActor, TRequest> repository = GetRepositoryStruct<TActor, TRequest>();

        return await repository.GracefulShutdown(actorRef, maxWait);
    }

    /// <summary>
    /// Returns the repository where the current references to request/response actors are stored.
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    /// <returns></returns>
    public ActorRepository<TActor, TRequest, TResponse> GetRepository<TActor, TRequest, TResponse>()
        where TActor : IActor<TRequest, TResponse> where TRequest : class where TResponse : class?
    {
        Lazy<IActorRepositoryRunnable> repository = repositories.GetOrAdd(
            typeof(TActor),
            (type) => new(CreateRepository<TActor, TRequest, TResponse>)
        );

        return (ActorRepository<TActor, TRequest, TResponse>)repository.Value;
    }

    private ActorRepository<TActor, TRequest, TResponse> CreateRepository<TActor, TRequest, TResponse>()
        where TActor : IActor<TRequest, TResponse> where TRequest : class where TResponse : class?
    {
        ActorRepository<TActor, TRequest, TResponse> repository = new(this, serviceProvider, logger);
        return repository;
    }

    /// <summary>
    /// Returns the repository where the current references to fire-n-forget actors are stored.
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <returns></returns>
    public ActorRepository<TActor, TRequest> GetRepository<TActor, TRequest>()
        where TActor : IActor<TRequest> where TRequest : class
    {
        Lazy<IActorRepositoryRunnable> repository = repositories.GetOrAdd(
            typeof(TActor),
            CreateRepositoryInternal<TActor, TRequest>
        );

        return (ActorRepository<TActor, TRequest>)repository.Value;
    }

    private Lazy<IActorRepositoryRunnable> CreateRepositoryInternal<TActor, TRequest>(Type type)
        where TActor : IActor<TRequest> where TRequest : class
    {
        return new(CreateRepositoryBuilder<TActor, TRequest>);
    }

    private ActorRepository<TActor, TRequest> CreateRepositoryBuilder<TActor, TRequest>()
        where TActor : IActor<TRequest> where TRequest : class
    {
        ActorRepository<TActor, TRequest> repository = new(this, serviceProvider, logger);
        return repository;
    }
    
    /// <summary>
    /// Returns the repository where the current references to fire-n-forget actors are stored.
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <returns></returns>
    public ActorRepositoryAggregate<TActor, TRequest> GetRepositoryAggregate<TActor, TRequest>()
        where TActor : IActorAggregate<TRequest> where TRequest : class
    {
        Lazy<IActorRepositoryRunnable> repository = repositories.GetOrAdd(
            typeof(TActor),
            CreateRepositoryInternalAggregate<TActor, TRequest>
        );

        return (ActorRepositoryAggregate<TActor, TRequest>)repository.Value;
    }
    
    private Lazy<IActorRepositoryRunnable> CreateRepositoryInternalAggregate<TActor, TRequest>(Type type)
        where TActor : IActorAggregate<TRequest> where TRequest : class
    {
        return new(CreateRepositoryBuilderAggreggate<TActor, TRequest>);
    }
    
    private ActorRepositoryAggregate<TActor, TRequest> CreateRepositoryBuilderAggreggate<TActor, TRequest>()
        where TActor : IActorAggregate<TRequest> where TRequest : class
    {
        ActorRepositoryAggregate<TActor, TRequest> repository = new(this, serviceProvider, logger);
        return repository;
    }

    /// <summary>
    /// Returns the repository where the current references to fire-n-forget actors are stored.
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <returns></returns>
    public ActorRepositoryStruct<TActor, TRequest> GetRepositoryStruct<TActor, TRequest>()
        where TActor : IActorStruct<TRequest> where TRequest : struct
    {
        Lazy<IActorRepositoryRunnable> repository = repositories.GetOrAdd(
            typeof(TActor),
            CreateRepositoryStructInternal<TActor, TRequest>
        );

        return (ActorRepositoryStruct<TActor, TRequest>)repository.Value;
    }

    private Lazy<IActorRepositoryRunnable> CreateRepositoryStructInternal<TActor, TRequest>(Type type)
        where TActor : IActorStruct<TRequest> where TRequest : struct
    {
        return new(CreateRepositoryStructBuilder<TActor, TRequest>);
    }

    private ActorRepositoryStruct<TActor, TRequest> CreateRepositoryStructBuilder<TActor, TRequest>()
        where TActor : IActorStruct<TRequest> where TRequest : struct
    {
        ActorRepositoryStruct<TActor, TRequest> repository = new(this, serviceProvider, logger);
        return repository;
    }

    /// <summary>
    /// Returns the repository where the current references to request/response actors are stored.
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    /// <returns></returns>
    public ActorRepositoryStruct<TActor, TRequest, TResponse> GetRepositoryStruct<TActor, TRequest, TResponse>()
        where TActor : IActorStruct<TRequest, TResponse> where TRequest : struct where TResponse : struct
    {
        Lazy<IActorRepositoryRunnable> repository = repositories.GetOrAdd(
            typeof(TActor),
            (type) => new(CreateRepositoryStruct<TActor, TRequest, TResponse>)
        );

        return (ActorRepositoryStruct<TActor, TRequest, TResponse>)repository.Value;
    }

    private ActorRepositoryStruct<TActor, TRequest, TResponse> CreateRepositoryStruct<TActor, TRequest, TResponse>()
        where TActor : IActorStruct<TRequest, TResponse> where TRequest : struct where TResponse : struct
    {
        ActorRepositoryStruct<TActor, TRequest, TResponse> repository = new(this, serviceProvider, logger);
        return repository;
    }
    
    /// <summary>
    /// Returns the repository where the current references to fire-n-forget actors are stored.
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <returns></returns>
    public ActorRepositoryAggregate<TActor, TRequest, TResponse> GetRepositoryAggregate<TActor, TRequest, TResponse>()
        where TActor : IActorAggregate<TRequest, TResponse> where TRequest : class where TResponse : class?
    {
        Lazy<IActorRepositoryRunnable> repository = repositories.GetOrAdd(
            typeof(TActor),
            CreateRepositoryInternalAggregate<TActor, TRequest, TResponse>
        );

        return (ActorRepositoryAggregate<TActor, TRequest, TResponse>)repository.Value;
    }
    
    private Lazy<IActorRepositoryRunnable> CreateRepositoryInternalAggregate<TActor, TRequest, TResponse>(Type type)
        where TActor : IActorAggregate<TRequest, TResponse> where TRequest : class where TResponse : class?
    {
        return new(CreateRepositoryBuilderAggreggate<TActor, TRequest, TResponse>);
    }
    
    private ActorRepositoryAggregate<TActor, TRequest, TResponse> CreateRepositoryBuilderAggreggate<TActor, TRequest, TResponse>()
        where TActor : IActorAggregate<TRequest, TResponse> where TRequest : class where TResponse : class?
    {
        ActorRepositoryAggregate<TActor, TRequest, TResponse> repository = new(this, serviceProvider, logger);
        return repository;
    }

    /// <summary>
    /// Creates a new periodic timer that will send a message to the specified actor every interval.
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <param name="name"></param>
    /// <param name="actorRef"></param>
    /// <param name="request"></param>
    /// <param name="initialDelay"></param>
    /// <param name="interval"></param>
    public void StartPeriodicTimer<TActor, TRequest>(IActorRef<TActor, TRequest> actorRef, string name, TRequest request, TimeSpan initialDelay, TimeSpan interval)
        where TActor : IActor<TRequest> where TRequest : class
    {
        scheduler.StartPeriodicTimer(actorRef, name, request, initialDelay, interval);
    }

    /// <summary>
    /// Creates a new periodic timer that will send a message to the specified actor every interval.
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    /// <param name="name"></param>
    /// <param name="actorRef"></param>
    /// <param name="request"></param>
    /// <param name="initialDelay"></param>
    /// <param name="interval"></param>
    public void StartPeriodicTimer<TActor, TRequest, TResponse>(IActorRef<TActor, TRequest, TResponse> actorRef, string name, TRequest request, TimeSpan initialDelay, TimeSpan interval)
        where TActor : IActor<TRequest, TResponse> where TRequest : class where TResponse : class?
    {
        scheduler.StartPeriodicTimer(actorRef, name, request, initialDelay, interval);
    }
    
    /// <summary>
    /// Creates a new periodic timer that will send a message to the specified actor every interval.
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <param name="name"></param>
    /// <param name="actorRef"></param>
    /// <param name="request"></param>
    /// <param name="initialDelay"></param>
    /// <param name="interval"></param>
    public void StartPeriodicTimerStruct<TActor, TRequest>(IActorRefStruct<TActor, TRequest> actorRef, string name, TRequest request, TimeSpan initialDelay, TimeSpan interval)
        where TActor : IActorStruct<TRequest> where TRequest : struct
    {
        scheduler.StartPeriodicTimerStruct(actorRef, name, request, initialDelay, interval);
    }
    
    /// <summary>
    /// Creates a new periodic timer that will send a message to the specified actor every interval.
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    /// <param name="name"></param>
    /// <param name="actorRef"></param>
    /// <param name="request"></param>
    /// <param name="initialDelay"></param>
    /// <param name="interval"></param>
    public void StartPeriodicTimerStruct<TActor, TRequest, TResponse>(IActorRefStruct<TActor, TRequest, TResponse> actorRef, string name, TRequest request, TimeSpan initialDelay, TimeSpan interval)
        where TActor : IActorStruct<TRequest, TResponse> where TRequest : struct where TResponse : struct
    {
        scheduler.StartPeriodicTimerStruct(actorRef, name, request, initialDelay, interval);
    }

    /// <summary>
    /// Schedules a message to be sent to an actor once after a specified delay.
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    /// <param name="actorRef"></param>
    /// <param name="request"></param>
    /// <param name="delay"></param>
    /// <returns></returns>
    public void ScheduleOnce<TActor, TRequest, TResponse>(IActorRef<TActor, TRequest, TResponse> actorRef, TRequest request, TimeSpan delay)
        where TActor : IActor<TRequest, TResponse> where TRequest : class where TResponse : class?
    {
        scheduler.ScheduleOnce(actorRef, request, delay);
    }

    /// <summary>
    /// Schedules a message to be sent to an actor once after a specified delay.
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <param name="actorRef"></param>
    /// <param name="request"></param>
    /// <param name="delay"></param>
    /// <returns></returns>
    public void ScheduleOnce<TActor, TRequest>(IActorRef<TActor, TRequest> actorRef, TRequest request, TimeSpan delay)
        where TActor : IActor<TRequest> where TRequest : class
    {
        scheduler.ScheduleOnce(actorRef, request, delay);
    }

    /// <summary>
    /// Schedules a message to be sent to an actor once after a specified delay.
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    /// <param name="actorRef"></param>
    /// <param name="request"></param>
    /// <param name="delay"></param>
    /// <returns></returns>
    public void ScheduleOnceStruct<TActor, TRequest, TResponse>(IActorRefStruct<TActor, TRequest, TResponse> actorRef, TRequest request, TimeSpan delay)
        where TActor : IActorStruct<TRequest, TResponse> where TRequest : struct where TResponse : struct
    {
        scheduler.ScheduleOnceStruct(actorRef, request, delay);
    }

    /// <summary>
    /// Schedules a message to be sent to an actor once after a specified delay.
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <param name="actorRef"></param>
    /// <param name="request"></param>
    /// <param name="delay"></param>
    /// <returns></returns>
    public void ScheduleOnceStruct<TActor, TRequest>(IActorRefStruct<TActor, TRequest> actorRef, TRequest request, TimeSpan delay)
        where TActor : IActorStruct<TRequest> where TRequest : struct
    {
        scheduler.ScheduleOnceStruct(actorRef, request, delay);
    }
    
    /// <summary>
    /// Schedule an actor to be terminated after the specified delay.
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    /// <param name="actorRef"></param>
    /// <param name="request"></param>
    /// <param name="delay"></param>
    /// <returns></returns>
    public void ScheduleShutdown<TActor, TRequest, TResponse>(IActorRef<TActor, TRequest, TResponse> actorRef, TimeSpan delay)
        where TActor : IActor<TRequest, TResponse> where TRequest : class where TResponse : class?
    {
        scheduler.ScheduleShutdown(actorRef, delay);
    }
    
    /// <summary>
    /// Schedule an actor to be terminated after the specified delay.
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    /// <param name="actorRef"></param>
    /// <param name="request"></param>
    /// <param name="delay"></param>
    /// <returns></returns>
    public void ScheduleShutdown<TActor, TRequest>(IActorRef<TActor, TRequest> actorRef, TimeSpan delay)
        where TActor : IActor<TRequest> where TRequest : class
    {
        scheduler.ScheduleShutdown(actorRef, delay);
    }

    /// <summary>
    /// Stops a periodic timer
    /// </summary>
    /// <param name="name"></param>
    /// <exception cref="NixieException"></exception>
    public void StopPeriodicTimer<TActor, TRequest>(IActorRef<TActor, TRequest> actorRef, string name) where TActor : IActor<TRequest> where TRequest : class
    {
        scheduler.StopPeriodicTimer(actorRef, name);
    }

    /// <summary>
    /// Stops a periodic timer
    /// </summary>
    /// <param name="name"></param>
    /// <exception cref="NixieException"></exception>
    public void StopPeriodicTimer<TActor, TRequest, TResponse>(IActorRef<TActor, TRequest, TResponse> actorRef, string name) where TActor : IActor<TRequest, TResponse> where TRequest : class where TResponse : class?
    {
        scheduler.StopPeriodicTimer(actorRef, name);
    }

    /// <summary>
    /// Stops all timers running or scheduled for the specified actor.
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    /// <param name="actorRef"></param>
    public void StopAllTimers<TActor, TRequest, TResponse>(IActorRef<TActor, TRequest, TResponse> actorRef) where TActor : IActor<TRequest, TResponse> where TRequest : class where TResponse : class?
    {
        scheduler.StopAllTimers(actorRef);
    }

    /// <summary>
    /// Stops all timers running or scheduled for the specified actor.
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <param name="actorRef"></param>
    public void StopAllTimers<TActor, TRequest>(IActorRef<TActor, TRequest> actorRef)
        where TActor : IActor<TRequest> where TRequest : class
    {
        scheduler.StopAllTimers(actorRef);
    }

    /// <summary>
    /// Stops all timers running or scheduled for the specified actor.
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <param name="actorRef"></param>
    public void StopAllTimers<TActor, TRequest>(IActorRefStruct<TActor, TRequest> actorRef)
        where TActor : IActorStruct<TRequest> where TRequest : struct
    {
        scheduler.StopAllTimers(actorRef);
    }

    /// <summary>
    /// Stops all timers running or scheduled for the specified actor.
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    /// <param name="actorRef"></param>
    public void StopAllTimers<TActor, TRequest, TResponse>(IActorRefStruct<TActor, TRequest, TResponse> actorRef)
        where TActor : IActorStruct<TRequest, TResponse> where TRequest : struct where TResponse : struct
    {
        scheduler.StopAllTimers(actorRef);
    }

    /// <summary>
    /// Waits for all the actors in the system to finish processing their messages.
    /// </summary>
    /// <returns></returns>
    public async Task Wait()
    {
        ValueStopwatch stopWatch = ValueStopwatch.StartNew();
        string? pendingActorName = null, processingName = null;

        while (true)
        {
            bool completed = true;

            foreach (KeyValuePair<Type, Lazy<IActorRepositoryRunnable>> repository in repositories)
            {
                Lazy<IActorRepositoryRunnable> lazyRepository = repository.Value;

                if (!lazyRepository.IsValueCreated)
                    continue;

                if (lazyRepository.Value.HasPendingMessages(out pendingActorName) || lazyRepository.Value.IsProcessing(out processingName))
                {
                    await Task.Yield();
                    completed = false;
                    break;
                }
            }

            if (completed)
                break;

            if (stopWatch.GetElapsedMilliseconds() > 11000)
            {
                logger?.LogWarning("Timeout waiting for actor {PendingActorName}/{ProcessingName}", pendingActorName, processingName);
                break;
            }
        }
    }

    /// <summary>
    /// Signals all actors in every repository to stop accepting messages and waits up to
    /// <paramref name="maxWait"/> for in-flight processing to complete before returning.
    /// Call this before <see cref="Dispose"/> to ensure a clean, deterministic teardown.
    /// </summary>
    public async Task GracefulShutdownAll(TimeSpan maxWait)
    {
        List<Task> tasks = new(repositories.Count);

        foreach (KeyValuePair<Type, Lazy<IActorRepositoryRunnable>> kv in repositories)
            tasks.Add(kv.Value.Value.GracefulShutdownAll(maxWait));

        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    public void Dispose()
    {
        scheduler.Dispose();
    }
}
