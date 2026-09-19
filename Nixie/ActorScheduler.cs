
using System.Diagnostics.CodeAnalysis;
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Nixie;

/// <summary>
/// Schedules messages to be sent to actors at a specified interval.
/// </summary>
public class ActorScheduler : IDisposable
{
    private long sequence;

    private readonly ActorSystem actorSystem;

    private readonly ILogger? logger;

    private readonly ConcurrentDictionary<object, Lazy<ConcurrentDictionary<long, Lazy<Timer>>>> onceTimers = new();

    private readonly ConcurrentDictionary<object, Lazy<ConcurrentDictionary<string, Lazy<Timer>>>> periodicTimers = new();

    public ActorScheduler(ActorSystem actorSystem, ILogger? logger)
    {
        this.actorSystem = actorSystem;
        this.logger = logger;
    }

    /// <summary>
    /// Schedules a message to be sent to an actor once after a specified delay at a specified interval.
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <param name="name"></param>
    /// <param name="actorRef"></param>
    /// <param name="request"></param>
    /// <param name="initialDelay"></param>
    /// <param name="interval"></param>
    public Timer StartPeriodicTimer<TActor, TRequest>(IActorRef<TActor, TRequest> actorRef, string name, TRequest request, TimeSpan initialDelay, TimeSpan interval)
        where TActor : IActor<TRequest> where TRequest : class
    {
        Lazy<ConcurrentDictionary<string, Lazy<Timer>>> timers = periodicTimers.GetOrAdd(actorRef, (_) => new());
        Lazy<Timer> created = new(() => AddPeriodicTimerInternal(actorRef, request, initialDelay, interval));
        Lazy<Timer> timer = timers.Value.GetOrAdd(name, created);

        // Identity check on the GetOrAdd result: a pre-existing entry means a timer with this name
        // is already active for this actor. (The old guard compared the name against the
        // actorRef-keyed outer map and could never fire.)
        if (!ReferenceEquals(timer, created))
            throw new NixieException("There is already an active timer with this name.");

        return timer.Value;
    }

    /// <summary>
    /// Schedules a message to be sent to an actor once after a specified delay at a specified interval.
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    /// <param name="actorRef"></param>
    /// <param name="name"></param>
    /// <param name="request"></param>
    /// <param name="initialDelay"></param>
    /// <param name="interval"></param>
    /// <returns></returns>
    /// <exception cref="NixieException"></exception>
    public Timer StartPeriodicTimer<TActor, TRequest, TResponse>(IActorRef<TActor, TRequest, TResponse> actorRef, string name, TRequest request, TimeSpan initialDelay, TimeSpan interval)
        where TActor : IActor<TRequest, TResponse> where TRequest : class where TResponse : class?
    {
        Lazy<ConcurrentDictionary<string, Lazy<Timer>>> timers = periodicTimers.GetOrAdd(actorRef, (_) => new());
        Lazy<Timer> created = new(() => AddPeriodicTimerInternal(actorRef, request, initialDelay, interval));
        Lazy<Timer> timer = timers.Value.GetOrAdd(name, created);

        if (!ReferenceEquals(timer, created))
            throw new NixieException("There is already an active timer with this name.");

        return timer.Value;
    }
    
    /// <summary>
    /// Schedules a message to be sent to an actor once after a specified delay at a specified interval.
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <param name="name"></param>
    /// <param name="actorRef"></param>
    /// <param name="request"></param>
    /// <param name="initialDelay"></param>
    /// <param name="interval"></param>
    public Timer StartPeriodicTimerStruct<TActor, TRequest>(IActorRefStruct<TActor, TRequest> actorRef, string name, TRequest request, TimeSpan initialDelay, TimeSpan interval)
        where TActor : IActorStruct<TRequest> where TRequest : struct
    {
        Lazy<ConcurrentDictionary<string, Lazy<Timer>>> timers = periodicTimers.GetOrAdd(actorRef, (_) => new());
        Lazy<Timer> created = new(() => AddPeriodicTimerInternalStruct(actorRef, request, initialDelay, interval));
        Lazy<Timer> timer = timers.Value.GetOrAdd(name, created);

        if (!ReferenceEquals(timer, created))
            throw new NixieException("There is already an active timer with this name.");

        return timer.Value;
    }

    /// <summary>
    /// Schedules a message to be sent to an actor once after a specified delay at a specified interval.
    /// </summary>
    /// <param name="actorRef"></param>
    /// <param name="name"></param>
    /// <param name="request"></param>
    /// <param name="initialDelay"></param>
    /// <param name="interval"></param>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    /// <returns></returns>
    /// <exception cref="NixieException"></exception>
    public Timer StartPeriodicTimerStruct<TActor, TRequest, TResponse>(IActorRefStruct<TActor, TRequest, TResponse> actorRef, string name, TRequest request, TimeSpan initialDelay, TimeSpan interval)
        where TActor : IActorStruct<TRequest, TResponse> where TRequest : struct where TResponse : struct
    {
        Lazy<ConcurrentDictionary<string, Lazy<Timer>>> timers = periodicTimers.GetOrAdd(actorRef, (_) => new());
        Lazy<Timer> created = new(() => AddPeriodicTimerInternalStruct(actorRef, request, initialDelay, interval));
        Lazy<Timer> timer = timers.Value.GetOrAdd(name, created);

        if (!ReferenceEquals(timer, created))
            throw new NixieException("There is already an active timer with this name.");

        return timer.Value;
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
    public Timer ScheduleOnce<TActor, TRequest, TResponse>(IActorRef<TActor, TRequest, TResponse> actorRef, TRequest request, TimeSpan delay)
        where TActor : IActor<TRequest, TResponse> where TRequest : class where TResponse : class?
    {
        long seq = Interlocked.Increment(ref sequence);
        Lazy<ConcurrentDictionary<long, Lazy<Timer>>> timers = onceTimers.GetOrAdd(actorRef, (object _) => new());
        Lazy<Timer> timer = timers.Value.GetOrAdd(seq, (long key) => new(() => ScheduleOnceTimer(actorRef, request, delay, seq)));
        return timer.Value;
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
    public Timer ScheduleOnce<TActor, TRequest>(IActorRef<TActor, TRequest> actorRef, TRequest request, TimeSpan delay)
        where TActor : IActor<TRequest> where TRequest : class
    {
        long seq = Interlocked.Increment(ref sequence);
        Lazy<ConcurrentDictionary<long, Lazy<Timer>>> timers = onceTimers.GetOrAdd(actorRef, (object _) => new());
        Lazy<Timer> timer = timers.Value.GetOrAdd(seq, (long key) => new(() => ScheduleOnceTimer(actorRef, request, delay, seq)));
        return timer.Value;
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
    public Timer ScheduleOnceStruct<TActor, TRequest, TResponse>(IActorRefStruct<TActor, TRequest, TResponse> actorRef, TRequest request, TimeSpan delay)
        where TActor : IActorStruct<TRequest, TResponse> where TRequest : struct where TResponse : struct
    {
        long seq = Interlocked.Increment(ref sequence);
        Lazy<ConcurrentDictionary<long, Lazy<Timer>>> timers = onceTimers.GetOrAdd(actorRef, (object _) => new());
        Lazy<Timer> timer = timers.Value.GetOrAdd(seq, (long key) => new(() => ScheduleOnceTimerStruct(actorRef, request, delay, seq)));
        return timer.Value;
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
    public Timer ScheduleOnceStruct<TActor, TRequest>(IActorRefStruct<TActor, TRequest> actorRef, TRequest request, TimeSpan delay)
        where TActor : IActorStruct<TRequest> where TRequest : struct
    {
        long seq = Interlocked.Increment(ref sequence);
        Lazy<ConcurrentDictionary<long, Lazy<Timer>>> timers = onceTimers.GetOrAdd(actorRef, (object _) => new());
        Lazy<Timer> timer = timers.Value.GetOrAdd(seq, (long key) => new(() => ScheduleOnceTimerStruct(actorRef, request, delay, seq)));
        return timer.Value;
    }
    
    /// <summary>
    /// Schedule an actor to be terminated after the specified delay.
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <param name="actorRef"></param>
    /// <param name="request"></param>
    /// <param name="delay"></param>
    /// <returns></returns>
    public Timer ScheduleShutdown<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest, TResponse>(IActorRef<TActor, TRequest, TResponse> actorRef, TimeSpan delay)
        where TActor : IActor<TRequest, TResponse> where TRequest : class where TResponse : class?
    {
        // Registered in onceTimers so the Timer stays rooted (System.Threading.Timer is not
        // self-rooting and could be GC-collected before firing) and is reachable for disposal
        // by StopAllTimers/Dispose. It removes itself after firing.
        long seq = Interlocked.Increment(ref sequence);
        Lazy<ConcurrentDictionary<long, Lazy<Timer>>> timers = onceTimers.GetOrAdd(actorRef, (object _) => new());
        Lazy<Timer> timer = timers.Value.GetOrAdd(seq, (long _) => new(() => new Timer((state) => ShutdownScheduled(actorRef, seq), null, delay, TimeSpan.Zero)));
        return timer.Value;
    }

    private void ShutdownScheduled<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest, TResponse>(IActorRef<TActor, TRequest, TResponse> actorRef, long seq)
        where TActor : IActor<TRequest, TResponse> where TRequest : class where TResponse : class?
    {
        try
        {
            actorSystem.Shutdown(actorRef);
            RemoveOnceTimer(actorRef, seq);
        }
        catch (Exception ex)
        {
            logger?.LogError("{Ex}", ex.Message);
        }
    }
    
    /// <summary>
    /// Schedule an actor to be terminated after the specified delay.
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <param name="actorRef"></param>
    /// <param name="request"></param>
    /// <param name="delay"></param>
    /// <returns></returns>
    public Timer ScheduleShutdown<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest>(IActorRef<TActor, TRequest> actorRef, TimeSpan delay)
        where TActor : IActor<TRequest> where TRequest : class
    {
        long seq = Interlocked.Increment(ref sequence);
        Lazy<ConcurrentDictionary<long, Lazy<Timer>>> timers = onceTimers.GetOrAdd(actorRef, (object _) => new());
        Lazy<Timer> timer = timers.Value.GetOrAdd(seq, (long _) => new(() => new Timer((state) => ShutdownScheduled(actorRef, seq), null, delay, TimeSpan.Zero)));
        return timer.Value;
    }

    private void ShutdownScheduled<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest>(IActorRef<TActor, TRequest> actorRef, long seq)
        where TActor : IActor<TRequest> where TRequest : class
    {
        try
        {
            actorSystem.Shutdown(actorRef);
            RemoveOnceTimer(actorRef, seq);
        }
        catch (Exception ex)
        {
            logger?.LogError("{Ex}", ex.Message);
        }
    }

    /// <summary>
    /// Stops a periodic timer
    /// </summary>
    /// <param name="name"></param>
    /// <exception cref="NixieException"></exception>
    public void StopPeriodicTimer<TActor, TRequest>(IActorRef<TActor, TRequest> actorRef, string name) where TActor : IActor<TRequest> where TRequest : class
    {
        if (periodicTimers.TryGetValue(actorRef, out Lazy<ConcurrentDictionary<string, Lazy<Timer>>>? timers))
        {
            if (!timers.Value.TryRemove(name, out Lazy<Timer>? timer))
                throw new NixieException("There is no timer with this name.");

            // Forcing Value (instead of checking IsValueCreated) closes the race with a starter
            // that inserted the Lazy but has not materialized the Timer yet: skipping it would
            // leave an unstoppable orphan timer outside the map.
            timer.Value.Dispose();
        }
    }

    /// <summary>
    /// Stops a periodic timer
    /// </summary>
    /// <param name="name"></param>
    /// <exception cref="NixieException"></exception>
    public void StopPeriodicTimer<TActor, TRequest, TResponse>(IActorRef<TActor, TRequest, TResponse> actorRef, string name)
        where TActor : IActor<TRequest, TResponse> where TRequest : class where TResponse : class?
    {
        if (periodicTimers.TryGetValue(actorRef, out Lazy<ConcurrentDictionary<string, Lazy<Timer>>>? timers))
        {
            if (!timers.Value.TryRemove(name, out Lazy<Timer>? timer))
                throw new NixieException("There is no timer with this name.");

            timer.Value.Dispose();
        }
    }

    /// <summary>
    /// Stops all timers running in an actor
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    /// <param name="actorRef"></param>
    public void StopAllTimers<TActor, TRequest, TResponse>(IActorRef<TActor, TRequest, TResponse> actorRef)
        where TActor : IActor<TRequest, TResponse> where TRequest : class where TResponse : class?
    {
        StopAllTimersInternal(actorRef);
    }

    /// <summary>
    /// Stops all timers running in an actor
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <param name="actorRef"></param>
    public void StopAllTimers<TActor, TRequest>(IActorRef<TActor, TRequest> actorRef)
        where TActor : IActor<TRequest> where TRequest : class
    {
        StopAllTimersInternal(actorRef);
    }

    /// <summary>
    /// Stops all timers running in an actor
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <param name="actorRef"></param>
    public void StopAllTimers<TActor, TRequest>(IActorRefStruct<TActor, TRequest> actorRef)
        where TActor : IActorStruct<TRequest> where TRequest : struct
    {
        StopAllTimersInternal(actorRef);
    }

    /// <summary>
    /// Stops all timers running in an actor
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    /// <param name="actorRef"></param>
    public void StopAllTimers<TActor, TRequest, TResponse>(IActorRefStruct<TActor, TRequest, TResponse> actorRef)
        where TActor : IActorStruct<TRequest, TResponse> where TRequest : struct where TResponse : struct
    {
        StopAllTimersInternal(actorRef);
    }

    /// <summary>
    /// Stops all timers running in an actor
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <param name="actorRef"></param>
    public void StopAllTimers<TActor, TRequest>(IActorRefAggregate<TActor, TRequest> actorRef)
        where TActor : IActorAggregate<TRequest> where TRequest : class
    {
        StopAllTimersInternal(actorRef);
    }

    /// <summary>
    /// Stops all timers running in an actor
    /// </summary>
    /// <typeparam name="TActor"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    /// <param name="actorRef"></param>
    public void StopAllTimers<TActor, TRequest, TResponse>(IActorRefAggregate<TActor, TRequest, TResponse> actorRef)
        where TActor : IActorAggregate<TRequest, TResponse> where TRequest : class where TResponse : class?
    {
        StopAllTimersInternal(actorRef);
    }

    private Timer AddPeriodicTimerInternal<TActor, TRequest>(IActorRef<TActor, TRequest> actorRef, TRequest request, TimeSpan initialDelay, TimeSpan interval)
        where TActor : IActor<TRequest> where TRequest : class
    {
        return new((state) => SendScheduledMessage(actorRef, request, -1), null, initialDelay, interval);
    }

    private Timer AddPeriodicTimerInternal<TActor, TRequest, TResponse>(IActorRef<TActor, TRequest, TResponse> actorRef, TRequest request, TimeSpan initialDelay, TimeSpan interval)
        where TActor : IActor<TRequest, TResponse> where TRequest : class where TResponse : class?
    {
        return new((state) => SendScheduledMessage(actorRef, request, -1), null, initialDelay, interval);
    }
    
    private Timer AddPeriodicTimerInternalStruct<TActor, TRequest>(IActorRefStruct<TActor, TRequest> actorRef, TRequest request, TimeSpan initialDelay, TimeSpan interval)
        where TActor : IActorStruct<TRequest> where TRequest : struct
    {
        return new((state) => SendScheduledMessage(actorRef, request, -1), null, initialDelay, interval);
    }
    
    private Timer AddPeriodicTimerInternalStruct<TActor, TRequest, TResponse>(IActorRefStruct<TActor, TRequest, TResponse> actorRef, TRequest request, TimeSpan initialDelay, TimeSpan interval)
        where TActor : IActorStruct<TRequest, TResponse> where TRequest : struct where TResponse : struct
    {
        return new((state) => SendScheduledMessage(actorRef, request, -1), null, initialDelay, interval);
    }

    private Timer ScheduleOnceTimer<TActor, TRequest>(IActorRef<TActor, TRequest> actorRef, TRequest request, TimeSpan delay, long random)
        where TActor : IActor<TRequest> where TRequest : class
    {
        return new((state) => SendScheduledMessage(actorRef, request, random), null, delay, TimeSpan.Zero);
    }

    private Timer ScheduleOnceTimer<TActor, TRequest, TResponse>(IActorRef<TActor, TRequest, TResponse> actorRef, TRequest request, TimeSpan delay, long random)
        where TActor : IActor<TRequest, TResponse> where TRequest : class where TResponse : class?
    {
        return new((state) => SendScheduledMessage(actorRef, request, random), null, delay, TimeSpan.Zero);
    }

    private Timer ScheduleOnceTimerStruct<TActor, TRequest>(IActorRefStruct<TActor, TRequest> actorRef, TRequest request, TimeSpan delay, long random)
       where TActor : IActorStruct<TRequest> where TRequest : struct
    {
        return new((state) => SendScheduledMessage(actorRef, request, random), null, delay, TimeSpan.Zero);
    }

    private Timer ScheduleOnceTimerStruct<TActor, TRequest, TResponse>(IActorRefStruct<TActor, TRequest, TResponse> actorRef, TRequest request, TimeSpan delay, long random)
        where TActor : IActorStruct<TRequest, TResponse> where TRequest : struct where TResponse : struct
    {
        return new((state) => SendScheduledMessage(actorRef, request, random), null, delay, TimeSpan.Zero);
    }

    private void SendScheduledMessage<TActor, TRequest>(IActorRef<TActor, TRequest> actorRef, TRequest request, long random)
        where TActor : IActor<TRequest> where TRequest : class
    {
        try
        {
            actorRef.Send(request);

            if (random > -1)
                RemoveOnceTimer(actorRef, random);
        }
        catch (Exception ex)
        {
            logger?.LogError("{Ex}", ex.Message);
        }
    }

    private void SendScheduledMessage<TActor, TRequest, TResponse>(IActorRef<TActor, TRequest, TResponse> actorRef, TRequest request, long random)
        where TActor : IActor<TRequest, TResponse> where TRequest : class where TResponse : class?
    {
        try
        {
            actorRef.Send(request);

            if (random > -1)
                RemoveOnceTimer(actorRef, random);
        }
        catch (Exception ex)
        {
            logger?.LogError("{Ex}", ex.Message);
        }
    }
    
    private void SendScheduledMessage<TActor, TRequest>(IActorRefStruct<TActor, TRequest> actorRef, TRequest request, long random)
        where TActor : IActorStruct<TRequest> where TRequest : struct
    {
        try
        {
            actorRef.Send(request);

            if (random > -1)
                RemoveOnceTimer(actorRef, random);
        }
        catch (Exception ex)
        {
            logger?.LogError("{Ex}", ex.Message);
        }
    }
    
    private void SendScheduledMessage<TActor, TRequest, TResponse>(IActorRefStruct<TActor, TRequest, TResponse> actorRef, TRequest request, long random)
        where TActor : IActorStruct<TRequest, TResponse> where TRequest : struct where TResponse : struct
    {
        try
        {
            actorRef.Send(request);

            if (random > -1)
                RemoveOnceTimer(actorRef, random);
        }
        catch (Exception ex)
        {
            logger?.LogError("{Ex}", ex.Message);
        }
    }

    /// <summary>
    /// Removes and disposes a once-timer after it fired or when its actor is being stopped.
    /// </summary>
    private void RemoveOnceTimer(object actorRef, long seq)
    {
        if (!onceTimers.TryGetValue(actorRef, out Lazy<ConcurrentDictionary<long, Lazy<Timer>>>? timers))
            return;

        if (!timers.Value.TryRemove(seq, out Lazy<Timer>? timer))
            return;

        // Forcing Value closes the race where this runs before the scheduling thread has
        // materialized the Lazy: the entry would otherwise leave the map with its timer
        // still alive and unreachable for disposal.
        timer.Value.Dispose();
    }

    private void StopAllTimersInternal(object actorRef)
    {
        if (periodicTimers.TryRemove(actorRef, out Lazy<ConcurrentDictionary<string, Lazy<Timer>>>? actorPeriodicTimers))
        {
            // Value is forced everywhere below (instead of checking IsValueCreated) so a timer whose
            // Lazy was inserted but not yet materialized by its starter is still disposed rather than
            // leaked outside the map as an unstoppable orphan.
            foreach (KeyValuePair<string, Lazy<Timer>> periodicTimer in actorPeriodicTimers.Value)
                periodicTimer.Value.Value.Dispose();
        }

        if (onceTimers.TryRemove(actorRef, out Lazy<ConcurrentDictionary<long, Lazy<Timer>>>? actorOnceTimers))
        {
            foreach (KeyValuePair<long, Lazy<Timer>> onceTimer in actorOnceTimers.Value)
                onceTimer.Value.Value.Dispose();
        }
    }

    public void Dispose()
    {
        foreach (KeyValuePair<object, Lazy<ConcurrentDictionary<string, Lazy<Timer>>>> periodicTimer in periodicTimers)
        {
            foreach (KeyValuePair<string, Lazy<Timer>> timer in periodicTimer.Value.Value)
                timer.Value.Value.Dispose();
        }

        periodicTimers.Clear();

        foreach (KeyValuePair<object, Lazy<ConcurrentDictionary<long, Lazy<Timer>>>> actorOnceTimer in onceTimers)
        {
            if (!actorOnceTimer.Value.IsValueCreated)
                continue;

            foreach (KeyValuePair<long, Lazy<Timer>> onceTimer in actorOnceTimer.Value.Value)
                onceTimer.Value.Value.Dispose();
        }

        onceTimers.Clear();
    }
}
