
using System.Diagnostics.CodeAnalysis;
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
    public IActorRef<TActor, TRequest, TResponse> Spawn<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest, TResponse>(string? name = null, params object[]? args)
        where TActor : IActor<TRequest, TResponse> where TRequest : class where TResponse : class?
    {
        ActorRepository<TActor, TRequest, TResponse> repository = GetRepository<TActor, TRequest, TResponse>();

        return repository.Spawn(name, args);
    }

    /// <summary>
    /// Creates a new request/response actor with options (e.g. a bounded inbox) and returns a typed reference.
    /// </summary>
    public IActorRef<TActor, TRequest, TResponse> SpawnWithOptions<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest, TResponse>(string? name, ActorRunnerOptions? options, params object[]? args)
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
    public IActorRefAggregate<TActor, TRequest> SpawnAggregate<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest>(string? name = null, params object[]? args)
        where TActor : IActorAggregate<TRequest> where TRequest : class
    {
        ActorRepositoryAggregate<TActor, TRequest> repository = GetRepositoryAggregate<TActor, TRequest>();

        return repository.Spawn(name, args);
    }

    /// <summary>
    /// Creates a new fire-n-forget aggregate actor with options (e.g. a bounded inbox) and returns a typed reference.
    /// </summary>
    public IActorRefAggregate<TActor, TRequest> SpawnAggregateWithOptions<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest>(string? name, ActorRunnerOptions? options, params object[]? args)
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
    public IActorRef<TActor, TRequest> Spawn<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest>(string? name = null, params object[]? args)
        where TActor : IActor<TRequest> where TRequest : class
    {
        ActorRepository<TActor, TRequest> repository = GetRepository<TActor, TRequest>();

        return repository.Spawn(name, args);
    }

    /// <summary>
    /// Creates a new fire-n-forget actor with options (e.g. a bounded inbox) and returns a typed reference.
    /// </summary>
    public IActorRef<TActor, TRequest> SpawnWithOptions<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest>(string? name, ActorRunnerOptions? options, params object[]? args)
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
    public IActorRefStruct<TActor, TRequest, TResponse> SpawnStruct<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest, TResponse>(string? name = null, params object[]? args)
        where TActor : IActorStruct<TRequest, TResponse> where TRequest : struct where TResponse : struct
    {
        ActorRepositoryStruct<TActor, TRequest, TResponse> repository = GetRepositoryStruct<TActor, TRequest, TResponse>();

        return repository.Spawn(name, args);
    }

    /// <summary>
    /// Creates a new request/response struct actor with options (e.g. a bounded inbox) and returns a typed reference.
    /// </summary>
    public IActorRefStruct<TActor, TRequest, TResponse> SpawnStructWithOptions<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest, TResponse>(string? name, ActorRunnerOptions? options, params object[]? args)
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
    public IActorRefStruct<TActor, TRequest> SpawnStruct<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest>(string? name = null, params object[]? args)
        where TActor : IActorStruct<TRequest> where TRequest : struct
    {
        ActorRepositoryStruct<TActor, TRequest> repository = GetRepositoryStruct<TActor, TRequest>();

        return repository.Spawn(name, args);
    }

    /// <summary>
    /// Creates a new fire-n-forget struct actor with options (e.g. a bounded inbox) and returns a typed reference.
    /// </summary>
    public IActorRefStruct<TActor, TRequest> SpawnStructWithOptions<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest>(string? name, ActorRunnerOptions? options, params object[]? args)
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
    public IActorRefAggregate<TActor, TRequest, TResponse> SpawnAggregate<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest, TResponse>(string? name = null, params object[]? args)
        where TActor : IActorAggregate<TRequest, TResponse> where TRequest : class where TResponse : class?
    {
        ActorRepositoryAggregate<TActor, TRequest, TResponse> repository = GetRepositoryAggregate<TActor, TRequest, TResponse>();

        return repository.Spawn(name, args);
    }

    /// <summary>
    /// Creates a new request/response aggregate actor with options (e.g. a bounded inbox) and returns a typed reference.
    /// </summary>
    public IActorRefAggregate<TActor, TRequest, TResponse> SpawnAggregateWithOptions<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest, TResponse>(string? name, ActorRunnerOptions? options, params object[]? args)
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
    public IActorRef<TActor, TRequest, TResponse>? Get<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest, TResponse>(string name)
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
    public IActorRef<TActor, TRequest>? Get<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest>(string name) where TActor : IActor<TRequest>
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
    public IActorRefStruct<TActor, TRequest>? GetStruct<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest>(string name) where TActor : IActorStruct<TRequest>
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
    public bool Shutdown<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest, TResponse>(string name) where TActor : IActor<TRequest, TResponse>
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
    public bool Shutdown<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest, TResponse>(IActorRef<TActor, TRequest, TResponse> actorRef)
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
    public bool Shutdown<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest>(string name) where TActor : IActor<TRequest>
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
    public bool Shutdown<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest>(IActorRef<TActor, TRequest> actorRef)
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
    public bool ShutdownStruct<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest, TResponse>(IActorRefStruct<TActor, TRequest, TResponse> actorRef)
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
    public bool ShutdownStruct<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest>(string name) where TActor : IActorStruct<TRequest>
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
    public bool ShutdownStruct<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest>(IActorRefStruct<TActor, TRequest> actorRef)
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
    public async Task<bool> GracefulShutdown<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest>(string name, TimeSpan maxWait) where TActor : IActor<TRequest>
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
    public async Task<bool> GracefulShutdown<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest, TResponse>(string name, TimeSpan maxWait) where TActor : IActor<TRequest, TResponse>
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
    public async Task<bool> GracefulShutdown<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest>(IActorRef<TActor, TRequest> actorRef, TimeSpan maxWait)
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
    public async Task<bool> GracefulShutdown<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest, TResponse>(IActorRef<TActor, TRequest, TResponse> actorRef, TimeSpan maxWait)
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
    public async Task<bool> GracefulShutdownStruct<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest>(string name, TimeSpan maxWait) where TActor : IActorStruct<TRequest>
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
    public async Task<bool> GracefulShutdownStruct<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest, TResponse>(IActorRefStruct<TActor, TRequest, TResponse> actorRef, TimeSpan maxWait)
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
    public async Task<bool> GracefulShutdownStruct<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest>(IActorRefStruct<TActor, TRequest> actorRef, TimeSpan maxWait)
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
    public ActorRepository<TActor, TRequest, TResponse> GetRepository<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest, TResponse>()
        where TActor : IActor<TRequest, TResponse> where TRequest : class where TResponse : class?
    {
        // Keyed by the closed repository type, not typeof(TActor): one actor class can be used
        // through several repository shapes (or request types), and a TActor-only key would hand
        // back a repository of the wrong closed type and fail the cast below.
        Type key = typeof(ActorRepository<TActor, TRequest, TResponse>);

        // A cache hit must not build a factory delegate. The argument to GetOrAdd is created
        // before the dictionary knows the key exists, so every lookup allocated a delegate that
        // only a first lookup can use. The Lazy still guarantees one repository per key.
        if (!repositories.TryGetValue(key, out Lazy<IActorRepositoryRunnable>? repository))
            repository = repositories.GetOrAdd(key, (type) => new(CreateRepository<TActor, TRequest, TResponse>));

        return (ActorRepository<TActor, TRequest, TResponse>)repository.Value;
    }

    private ActorRepository<TActor, TRequest, TResponse> CreateRepository<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest, TResponse>()
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
    public ActorRepository<TActor, TRequest> GetRepository<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest>()
        where TActor : IActor<TRequest> where TRequest : class
    {
        Type key = typeof(ActorRepository<TActor, TRequest>);

        // A cache hit must not build a factory delegate. The argument to GetOrAdd is created
        // before the dictionary knows the key exists, so every lookup allocated a delegate that
        // only a first lookup can use. The Lazy still guarantees one repository per key.
        if (!repositories.TryGetValue(key, out Lazy<IActorRepositoryRunnable>? repository))
            repository = repositories.GetOrAdd(key, CreateRepositoryInternal<TActor, TRequest>);

        return (ActorRepository<TActor, TRequest>)repository.Value;
    }

    private Lazy<IActorRepositoryRunnable> CreateRepositoryInternal<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest>(Type type)
        where TActor : IActor<TRequest> where TRequest : class
    {
        return new(CreateRepositoryBuilder<TActor, TRequest>);
    }

    private ActorRepository<TActor, TRequest> CreateRepositoryBuilder<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest>()
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
    public ActorRepositoryAggregate<TActor, TRequest> GetRepositoryAggregate<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest>()
        where TActor : IActorAggregate<TRequest> where TRequest : class
    {
        Type key = typeof(ActorRepositoryAggregate<TActor, TRequest>);

        // A cache hit must not build a factory delegate. The argument to GetOrAdd is created
        // before the dictionary knows the key exists, so every lookup allocated a delegate that
        // only a first lookup can use. The Lazy still guarantees one repository per key.
        if (!repositories.TryGetValue(key, out Lazy<IActorRepositoryRunnable>? repository))
            repository = repositories.GetOrAdd(key, CreateRepositoryInternalAggregate<TActor, TRequest>);

        return (ActorRepositoryAggregate<TActor, TRequest>)repository.Value;
    }
    
    private Lazy<IActorRepositoryRunnable> CreateRepositoryInternalAggregate<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest>(Type type)
        where TActor : IActorAggregate<TRequest> where TRequest : class
    {
        return new(CreateRepositoryBuilderAggreggate<TActor, TRequest>);
    }
    
    private ActorRepositoryAggregate<TActor, TRequest> CreateRepositoryBuilderAggreggate<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest>()
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
    public ActorRepositoryStruct<TActor, TRequest> GetRepositoryStruct<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest>()
        where TActor : IActorStruct<TRequest> where TRequest : struct
    {
        Type key = typeof(ActorRepositoryStruct<TActor, TRequest>);

        // A cache hit must not build a factory delegate. The argument to GetOrAdd is created
        // before the dictionary knows the key exists, so every lookup allocated a delegate that
        // only a first lookup can use. The Lazy still guarantees one repository per key.
        if (!repositories.TryGetValue(key, out Lazy<IActorRepositoryRunnable>? repository))
            repository = repositories.GetOrAdd(key, CreateRepositoryStructInternal<TActor, TRequest>);

        return (ActorRepositoryStruct<TActor, TRequest>)repository.Value;
    }

    private Lazy<IActorRepositoryRunnable> CreateRepositoryStructInternal<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest>(Type type)
        where TActor : IActorStruct<TRequest> where TRequest : struct
    {
        return new(CreateRepositoryStructBuilder<TActor, TRequest>);
    }

    private ActorRepositoryStruct<TActor, TRequest> CreateRepositoryStructBuilder<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest>()
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
    public ActorRepositoryStruct<TActor, TRequest, TResponse> GetRepositoryStruct<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest, TResponse>()
        where TActor : IActorStruct<TRequest, TResponse> where TRequest : struct where TResponse : struct
    {
        Type key = typeof(ActorRepositoryStruct<TActor, TRequest, TResponse>);

        // A cache hit must not build a factory delegate. The argument to GetOrAdd is created
        // before the dictionary knows the key exists, so every lookup allocated a delegate that
        // only a first lookup can use. The Lazy still guarantees one repository per key.
        if (!repositories.TryGetValue(key, out Lazy<IActorRepositoryRunnable>? repository))
            repository = repositories.GetOrAdd(key, (type) => new(CreateRepositoryStruct<TActor, TRequest, TResponse>));

        return (ActorRepositoryStruct<TActor, TRequest, TResponse>)repository.Value;
    }

    private ActorRepositoryStruct<TActor, TRequest, TResponse> CreateRepositoryStruct<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest, TResponse>()
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
    public ActorRepositoryAggregate<TActor, TRequest, TResponse> GetRepositoryAggregate<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest, TResponse>()
        where TActor : IActorAggregate<TRequest, TResponse> where TRequest : class where TResponse : class?
    {
        Type key = typeof(ActorRepositoryAggregate<TActor, TRequest, TResponse>);

        // A cache hit must not build a factory delegate. The argument to GetOrAdd is created
        // before the dictionary knows the key exists, so every lookup allocated a delegate that
        // only a first lookup can use. The Lazy still guarantees one repository per key.
        if (!repositories.TryGetValue(key, out Lazy<IActorRepositoryRunnable>? repository))
            repository = repositories.GetOrAdd(key, CreateRepositoryInternalAggregate<TActor, TRequest, TResponse>);

        return (ActorRepositoryAggregate<TActor, TRequest, TResponse>)repository.Value;
    }
    
    private Lazy<IActorRepositoryRunnable> CreateRepositoryInternalAggregate<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest, TResponse>(Type type)
        where TActor : IActorAggregate<TRequest, TResponse> where TRequest : class where TResponse : class?
    {
        return new(CreateRepositoryBuilderAggreggate<TActor, TRequest, TResponse>);
    }
    
    private ActorRepositoryAggregate<TActor, TRequest, TResponse> CreateRepositoryBuilderAggreggate<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest, TResponse>()
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
    public void ScheduleShutdown<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest, TResponse>(IActorRef<TActor, TRequest, TResponse> actorRef, TimeSpan delay)
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
    public void ScheduleShutdown<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest>(IActorRef<TActor, TRequest> actorRef, TimeSpan delay)
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
    /// Stops all timers running or scheduled for the specified actor.
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <param name="actorRef"></param>
    public void StopAllTimers<TActor, TRequest>(IActorRefAggregate<TActor, TRequest> actorRef)
        where TActor : IActorAggregate<TRequest> where TRequest : class
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
    public void StopAllTimers<TActor, TRequest, TResponse>(IActorRefAggregate<TActor, TRequest, TResponse> actorRef)
        where TActor : IActorAggregate<TRequest, TResponse> where TRequest : class where TResponse : class?
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
        int rounds = 0;

        while (true)
        {
            bool completed = true;

            foreach (KeyValuePair<Type, Lazy<IActorRepositoryRunnable>> repository in repositories)
            {
                Lazy<IActorRepositoryRunnable> lazyRepository = repository.Value;

                if (!lazyRepository.IsValueCreated)
                    continue;

                // One scan per repository per round: the two separate checks enumerated every actor twice
                // to rediscover almost the same state.
                if (lazyRepository.Value.HasActivity(out pendingActorName, out processingName))
                {
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

            // Yield for the first rounds to stay responsive to fast drains, then back off to a
            // timed delay so a long wait doesn't hot-spin a core re-scanning every repository.
            if (++rounds < 128)
                await Task.Yield();
            else
                await Task.Delay(1);
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
        // Timers first so nothing fires into actors being torn down, then the actors themselves:
        // otherwise runners keep accepting and processing messages after the system is disposed
        // and PostShutdown hooks never run.
        scheduler.Dispose();

        foreach (KeyValuePair<Type, Lazy<IActorRepositoryRunnable>> kv in repositories)
        {
            if (kv.Value.IsValueCreated)
                kv.Value.Value.ShutdownAll();
        }

        repositories.Clear();
    }
}
