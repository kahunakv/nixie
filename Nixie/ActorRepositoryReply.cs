
using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Nixie;

/// <summary>
/// This class maintains an inventory of created actors of a specific type, as well as references to the runners that contain the mailboxes.
/// </summary>
/// <typeparam name="TActor"></typeparam>
/// <typeparam name="TRequest"></typeparam>
/// <typeparam name="TResponse"></typeparam>
public sealed class ActorRepository<TActor, TRequest, TResponse> : IActorRepositoryRunnable 
    where TActor : IActor<TRequest, TResponse> where TRequest : class where TResponse : class?
{
    private readonly ActorSystem actorSystem;

    private readonly IServiceProvider? serviceProvider;

    private readonly ILogger? logger;

    private readonly ConcurrentDictionary<string, Lazy<(ActorRunner<TActor, TRequest, TResponse>, ActorRef<TActor, TRequest, TResponse>)>> actors = new();

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="actorSystem"></param>
    /// <param name="serviceProvider"></param>
    /// <param name="logger"></param>
    public ActorRepository(ActorSystem actorSystem, IServiceProvider? serviceProvider, ILogger? logger)
    {
        this.actorSystem = actorSystem;
        this.serviceProvider = serviceProvider;
        this.logger = logger;
    }

    /// <summary>
    /// Check if any of the actors have pending messages
    /// </summary>
    /// <returns></returns>
    public bool HasPendingMessages(out string? actorName)
    {
        foreach (KeyValuePair<string, Lazy<(ActorRunner<TActor, TRequest, TResponse> runner, ActorRef<TActor, TRequest, TResponse> actorRef)>> actor in actors)
        {
            Lazy<(ActorRunner<TActor, TRequest, TResponse> runner, ActorRef<TActor, TRequest, TResponse> actorRef)> lazyValue = actor.Value;

            if (lazyValue.IsValueCreated)
            {
                ActorRunner<TActor, TRequest, TResponse> runner = lazyValue.Value.runner;

                if (!runner.IsShutdown && !lazyValue.Value.runner.IsEmpty)
                {
                    actorName = runner.Name;
                    return true;
                }
            }
        }

        actorName = null;
        return false;
    }

    /// <summary>
    /// Check if any of the actors are processing messages
    /// </summary>
    /// <returns></returns>
    public bool IsProcessing(out string? actorName)
    {
        foreach (KeyValuePair<string, Lazy<(ActorRunner<TActor, TRequest, TResponse> runner, ActorRef<TActor, TRequest, TResponse> actorRef)>> actor in actors)
        {
            Lazy<(ActorRunner<TActor, TRequest, TResponse> runner, ActorRef<TActor, TRequest, TResponse> actorRef)> lazyValue = actor.Value;

            if (lazyValue.IsValueCreated)
            {
                ActorRunner<TActor, TRequest, TResponse> runner = lazyValue.Value.runner;

                if (runner is { IsShutdown: false, IsProcessing: true })
                {
                    actorName = runner.Name;
                    return true;
                }
            }
        }

        actorName = null;
        return false;
    }

    /// <summary>
    /// Reports both kinds of activity in one scan, so a waiting caller does not enumerate the repository
    /// twice per round. Pending messages take precedence over a running handler, which is the order the
    /// two separate checks produced. Both names are null when the repository is idle.
    /// </summary>
    /// <param name="pendingActorName"></param>
    /// <param name="processingActorName"></param>
    /// <returns></returns>
    public bool HasActivity(out string? pendingActorName, out string? processingActorName)
    {
        string? firstProcessingName = null;

        foreach (KeyValuePair<string, Lazy<(ActorRunner<TActor, TRequest, TResponse> runner, ActorRef<TActor, TRequest, TResponse> actorRef)>> actor in actors)
        {
            Lazy<(ActorRunner<TActor, TRequest, TResponse> runner, ActorRef<TActor, TRequest, TResponse> actorRef)> lazyValue = actor.Value;

            if (!lazyValue.IsValueCreated)
                continue;

            ActorRunner<TActor, TRequest, TResponse> runner = lazyValue.Value.runner;

            if (runner.IsShutdown)
                continue;

            if (!runner.IsEmpty)
            {
                pendingActorName = runner.Name;
                processingActorName = null;
                return true;
            }

            if (firstProcessingName is null && runner.IsProcessing)
                firstProcessingName = runner.Name;
        }

        pendingActorName = null;
        processingActorName = firstProcessingName;
        return firstProcessingName is not null;
    }

    /// <summary>
    /// Creates a new actor and returns a reference to it
    /// </summary>
    /// <param name="name"></param>
    /// <param name="args"></param>
    /// <returns></returns>
    /// <exception cref="NixieException"></exception>
    public IActorRef<TActor, TRequest, TResponse> Spawn(string? name = null, params object[]? args)
        => SpawnWithOptions(name, null, args);

    public IActorRef<TActor, TRequest, TResponse> SpawnWithOptions(string? name, ActorRunnerOptions? options, params object[]? args)
    {
        if (!string.IsNullOrEmpty(name))
            name = name.ToLowerInvariant();
        else
            name = Guid.NewGuid().ToString();

        int? maxInboxSize = options?.MaxInboxSize;
        Func<object, bool>? isControlMessage = ResolveControlPredicate(options);

        Lazy<(ActorRunner<TActor, TRequest, TResponse> runner, ActorRef<TActor, TRequest, TResponse> actorRef)> created = new(() => CreateInternal(name, maxInboxSize, isControlMessage, args));
        Lazy<(ActorRunner<TActor, TRequest, TResponse> runner, ActorRef<TActor, TRequest, TResponse> actorRef)> actor = actors.GetOrAdd(name, created);

        // Identity check instead of a separate ContainsKey: two concurrent spawns of the same name
        // race deterministically — exactly one wins and the loser throws, instead of silently
        // receiving an actor built with the winner's constructor args.
        if (!ReferenceEquals(actor, created))
            throw new NixieException("Actor already exists");

        try
        {
            return actor.Value.actorRef;
        }
        catch
        {
            // A throwing actor constructor must not poison the name: Lazy caches the exception, so
            // the entry would otherwise reject every re-spawn and rethrow on every Get forever.
            actors.TryRemove(name, out _);
            throw;
        }
    }

    /// <summary>
    /// Resolves the control-message classifier. A typed <see cref="ActorRunnerOptions{TRequest}"/> is adapted
    /// with a reference cast (no boxing, since <typeparamref name="TRequest"/> is a class); otherwise the base
    /// <see cref="ActorRunnerOptions.IsControlMessage"/> is used directly.
    /// </summary>
    private static Func<object, bool>? ResolveControlPredicate(ActorRunnerOptions? options)
    {
        if (options is ActorRunnerOptions<TRequest> typed && typed.IsControlMessage is not null)
            return message => typed.IsControlMessage((TRequest)message);

        return options?.IsControlMessage;
    }

    private (ActorRunner<TActor, TRequest, TResponse>, ActorRef<TActor, TRequest, TResponse>) CreateInternal(string name, int? maxInboxSize, Func<object, bool>? isControlMessage, params object[]? args)
    {
        ActorRunner<TActor, TRequest, TResponse> runner = new(actorSystem, logger, name, maxInboxSize, isControlMessage);

        ActorRef<TActor, TRequest, TResponse> actorRef = new(runner);

        ActorContext<TActor, TRequest, TResponse> actorContext = new(actorSystem, logger, actorRef);

        TActor? actor;

        if (args is not null && args.Length > 0)
        {
            object[] arguments = new object[args.Length + 1];

            arguments[0] = actorContext;

            for (int i = 0; i < args.Length; i++)
                arguments[i + 1] = args[i];

            if (serviceProvider is not null)
                actor = (TActor?)ActivatorUtilities.CreateInstance(serviceProvider, typeof(TActor), arguments);
            else
                actor = (TActor?)Activator.CreateInstance(typeof(TActor), arguments);
        }
        else
        {
            if (serviceProvider is not null)
                actor = (TActor?)ActivatorUtilities.CreateInstance(serviceProvider, typeof(TActor), actorContext);
            else
                actor = (TActor?)Activator.CreateInstance(typeof(TActor), actorContext);
        }

        if (actor is null)
            throw new NixieException("Invalid actor");

        runner.ActorContext = actorContext;
        runner.Actor = actor;

        return (runner, actorRef);
    }

    /// <summary>
    /// Returns a reference to an existing actor
    /// </summary>
    /// <param name="name"></param>
    /// <returns></returns>
    public IActorRef<TActor, TRequest, TResponse>? Get(string name)
    {
        name = name.ToLowerInvariant();

        if (actors.TryGetValue(name, out Lazy<(ActorRunner<TActor, TRequest, TResponse> runner, ActorRef<TActor, TRequest, TResponse> actorRef)>? actor))
            return actor.Value.actorRef;

        return null;
    }

    /// <summary>
    /// Shutdowns an actor by its name
    /// </summary>
    /// <param name="name"></param>
    /// <returns></returns>
    public bool Shutdown(string name)
    {
        name = name.ToLowerInvariant();

        if (actors.TryGetValue(name, out Lazy<(ActorRunner<TActor, TRequest, TResponse> runner, ActorRef<TActor, TRequest, TResponse> actorRef)>? actor))
        {
            bool wasShutdown = actor.Value.runner.Shutdown();

            // Removed even when another path already shut the runner down, so a dead actor can't
            // stay resolvable via Get or block re-spawning its name.
            actorSystem.StopAllTimers(actor.Value.actorRef);
            actors.TryRemove(name, out _);
            return wasShutdown;
        }

        return true;
    }

    /// <summary>
    /// Shutdowns an actor by its reference
    /// </summary>
    /// <param name="actorRef"></param>
    /// <returns></returns>
    public bool Shutdown(IActorRef<TActor, TRequest, TResponse> actorRef)
    {
        string name = actorRef.Runner.Name;

        if (actors.TryGetValue(name, out Lazy<(ActorRunner<TActor, TRequest, TResponse> runner, ActorRef<TActor, TRequest, TResponse> actorRef)>? actor))
        {
            bool wasShutdown = actor.Value.runner.Shutdown();

            actorSystem.StopAllTimers(actor.Value.actorRef);
            actors.TryRemove(name, out _);
            return wasShutdown;
        }

        return true;
    }

    /// <summary>
    /// Tries to shutdown an actor by its name and returns a task whose result confirms shutdown within the specified timespan
    /// </summary>
    /// <param name="name"></param>
    /// <param name="maxWait"></param>
    /// <returns></returns>
    public async Task<bool> GracefulShutdown(string name, TimeSpan maxWait)
    {
        name = name.ToLowerInvariant();

        if (actors.TryGetValue(name, out Lazy<(ActorRunner<TActor, TRequest, TResponse> runner, ActorRef<TActor, TRequest, TResponse> actorRef)>? actor))
        {
            bool success = await actor.Value.runner.GracefulShutdown(maxWait);
            actors.TryRemove(name, out _);
            actorSystem.StopAllTimers(actor.Value.actorRef);
            return success;
        }

        return true;
    }
    
    /// <summary>
    /// Tries to shutdown an actor by its name and returns a task whose result confirms shutdown within the specified timespan
    /// </summary>
    /// <param name="actorRef"></param>
    /// <param name="maxWait"></param>
    /// <returns></returns>
    public async Task<bool> GracefulShutdown(IActorRef<TActor, TRequest, TResponse> actorRef, TimeSpan maxWait)
    {
        string name = actorRef.Runner.Name;

        if (actors.TryGetValue(name, out Lazy<(ActorRunner<TActor, TRequest, TResponse> runner, ActorRef<TActor, TRequest, TResponse> actorRef)>? actor))
        {
            bool success = await actor.Value.runner.GracefulShutdown(maxWait);
            actors.TryRemove(name, out _);
            actorSystem.StopAllTimers(actor.Value.actorRef);
            return success;
        }

        return true;
    }

    public async Task GracefulShutdownAll(TimeSpan maxWait)
    {
        List<Task<bool>> tasks = new(actors.Count);
        List<string> names = new(actors.Count);

        foreach (KeyValuePair<string, Lazy<(ActorRunner<TActor, TRequest, TResponse> runner, ActorRef<TActor, TRequest, TResponse> actorRef)>> kv in actors)
        {
            names.Add(kv.Key);
            tasks.Add(kv.Value.Value.runner.GracefulShutdown(maxWait).AsTask());
        }

        await Task.WhenAll(tasks).ConfigureAwait(false);

        // Remove only what this call shut down: Clear() would silently discard (and leave running)
        // actors spawned while the drain was in progress.
        foreach (string name in names)
            actors.TryRemove(name, out _);
    }

    public void ShutdownAll()
    {
        foreach (KeyValuePair<string, Lazy<(ActorRunner<TActor, TRequest, TResponse> runner, ActorRef<TActor, TRequest, TResponse> actorRef)>> kv in actors)
        {
            actors.TryRemove(kv.Key, out _);

            if (kv.Value.IsValueCreated)
                kv.Value.Value.runner.Shutdown();
        }
    }
}
