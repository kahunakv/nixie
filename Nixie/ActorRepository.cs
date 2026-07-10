
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
public sealed class ActorRepository<TActor, TRequest> : IActorRepositoryRunnable where TActor : IActor<TRequest> where TRequest : class
{
    private readonly ActorSystem actorSystem;

    private readonly IServiceProvider? serviceProvider;

    private readonly ILogger? logger;

    private readonly ConcurrentDictionary<string, Lazy<(ActorRunner<TActor, TRequest>, ActorRef<TActor, TRequest>)>> actors = new();

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="actorSystem"></param>
    /// <param name="serviceProvider"></param>
    public ActorRepository(ActorSystem actorSystem, IServiceProvider? serviceProvider, ILogger? logger)
    {
        this.actorSystem = actorSystem;
        this.serviceProvider = serviceProvider;
        this.logger = logger;
    }

    /// <summary>
    /// Check if any of the actors have pending messages
    /// </summary>
    /// <param name="actorName"></param>
    /// <returns></returns>
    public bool HasPendingMessages(out string? actorName)
    {        
        foreach (KeyValuePair<string, Lazy<(ActorRunner<TActor, TRequest> runner, ActorRef<TActor, TRequest> actorRef)>> actor in actors)
        {
            Lazy<(ActorRunner<TActor, TRequest> runner, ActorRef<TActor, TRequest> actorRef)> lazyValue = actor.Value;

            if (lazyValue.IsValueCreated)
            {
                ActorRunner<TActor, TRequest> runner = lazyValue.Value.runner;

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
    /// <param name="actorName"></param>
    /// <returns></returns>
    public bool IsProcessing(out string? actorName)
    {
        foreach (KeyValuePair<string, Lazy<(ActorRunner<TActor, TRequest> runner, ActorRef<TActor, TRequest> actorRef)>> actor in actors)
        {
            Lazy<(ActorRunner<TActor, TRequest> runner, ActorRef<TActor, TRequest> actorRef)> lazyValue = actor.Value;

            if (lazyValue.IsValueCreated)
            {
                ActorRunner<TActor, TRequest> runner = lazyValue.Value.runner;

                if (!runner.IsShutdown && runner.IsProcessing)
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
    /// Creates a new actor and returns a reference to it
    /// </summary>
    /// <param name="name"></param>
    /// <param name="args"></param>
    /// <returns></returns>
    /// <exception cref="NixieException"></exception>
    public IActorRef<TActor, TRequest> Spawn(string? name = null, params object[]? args)
        => SpawnWithOptions(name, null, args);

    public IActorRef<TActor, TRequest> SpawnWithOptions(string? name, ActorRunnerOptions? options, params object[]? args)
    {
        if (!string.IsNullOrEmpty(name))
        {
            name = name.ToLowerInvariant();

            if (actors.ContainsKey(name))
                throw new NixieException("Actor already exists");
        }
        else
        {
            name = Guid.NewGuid().ToString();
        }

        int? maxInboxSize = options?.MaxInboxSize;

        Lazy<(ActorRunner<TActor, TRequest> runner, ActorRef<TActor, TRequest> actorRef)> actor = actors.GetOrAdd(
            name,
            (string _) => new(() => CreateInternal(name, maxInboxSize, args))
        );

        return actor.Value.actorRef;
    }

    private (ActorRunner<TActor, TRequest>, ActorRef<TActor, TRequest>) CreateInternal(string name, int? maxInboxSize, params object[]? args)
    {
        ActorRunner<TActor, TRequest> runner = new(actorSystem, logger, name, maxInboxSize);

        ActorRef<TActor, TRequest>? actorRef = (ActorRef<TActor, TRequest>?)Activator.CreateInstance(typeof(ActorRef<TActor, TRequest>), runner);

        if (actorRef is null)
            throw new NixieException("Invalid actor props");

        ActorContext<TActor, TRequest> actorContext = new(actorSystem, logger, actorRef);

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
    public IActorRef<TActor, TRequest>? Get(string name)
    {
        name = name.ToLowerInvariant();

        if (actors.TryGetValue(name, out Lazy<(ActorRunner<TActor, TRequest> runner, ActorRef<TActor, TRequest> actorRef)>? actor))
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

        if (actors.TryGetValue(name, out Lazy<(ActorRunner<TActor, TRequest> runner, ActorRef<TActor, TRequest> actorRef)>? actor))
        {
            if (actor.Value.runner.Shutdown())
            {
                actorSystem.StopAllTimers(actor.Value.actorRef);
                actors.TryRemove(name, out _);
                return true;
            }
        }

        return true;
    }

    /// <summary>
    /// Shutdowns an actor by its reference
    /// </summary>
    /// <param name="name"></param>
    /// <returns></returns>
    public bool Shutdown(IActorRef<TActor, TRequest> actorRef)
    {
        string name = actorRef.Runner.Name;

        if (actors.TryGetValue(name, out Lazy<(ActorRunner<TActor, TRequest> runner, ActorRef<TActor, TRequest> actorRef)>? actor))
        {
            if (actor.Value.runner.Shutdown())
            {
                actorSystem.StopAllTimers(actor.Value.actorRef);
                actors.TryRemove(name, out _);
                return true;
            }
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

        if (actors.TryGetValue(name, out Lazy<(ActorRunner<TActor, TRequest> runner, ActorRef<TActor, TRequest> actorRef)>? actor))
        {
            bool result = await actor.Value.runner.GracefulShutdown(maxWait);
            actorSystem.StopAllTimers(actor.Value.actorRef);
            actors.TryRemove(name, out _);
            return result;
        }

        return true;
    }

    /// <summary>
    /// Tries to shutdown an actor by its name and returns a task whose result confirms shutdown within the specified timespan
    /// </summary>
    /// <param name="actorRef"></param>
    /// <param name="maxWait"></param>
    /// <returns></returns>
    public async Task<bool> GracefulShutdown(IActorRef<TActor, TRequest> actorRef, TimeSpan maxWait)
    {
        string name = actorRef.Runner.Name;

        if (actors.TryGetValue(name, out Lazy<(ActorRunner<TActor, TRequest> runner, ActorRef<TActor, TRequest> actorRef)>? actor))
        {
            bool success = await actor.Value.runner.GracefulShutdown(maxWait);
            actorSystem.StopAllTimers(actor.Value.actorRef);
            actors.TryRemove(name, out _);
            return success;
        }

        return true;
    }

    public async Task GracefulShutdownAll(TimeSpan maxWait)
    {
        List<Task<bool>> tasks = new(actors.Count);

        foreach (KeyValuePair<string, Lazy<(ActorRunner<TActor, TRequest> runner, ActorRef<TActor, TRequest> actorRef)>> kv in actors)
            tasks.Add(kv.Value.Value.runner.GracefulShutdown(maxWait).AsTask());

        await Task.WhenAll(tasks).ConfigureAwait(false);

        actors.Clear();
    }
}
