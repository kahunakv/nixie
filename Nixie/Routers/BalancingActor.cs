
using System.Diagnostics.CodeAnalysis;

namespace Nixie.Routers;

public class BalancingActor<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor, TRequest> : IActor<TRequest>
    where TActor : IActor<TRequest> where TRequest : class
{
    /// <summary>
    /// Router actor context
    /// </summary>
    private readonly IActorContext<BalancingActor<TActor, TRequest>, TRequest> context;

    /// <summary>
    /// Instances to send messages to
    /// </summary>
    private readonly List<IActorRef<TActor, TRequest>> instances = [];

    /// <summary>
    /// Returns the current list of instances
    /// </summary>
    public List<IActorRef<TActor, TRequest>> Instances => instances;
    
    /// <summary>
    /// Random number generator
    /// </summary>
    private readonly Random random = new();

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="context"></param>
    /// <param name="numberInstances"></param>
    public BalancingActor(
        IActorContext<BalancingActor<TActor, TRequest>, TRequest> context, 
        int numberInstances
    )
    {
        if (numberInstances < 1)
            throw new NixieException("A router must have at least one routee.");

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
    public BalancingActor(
        IActorContext<BalancingActor<TActor, TRequest>, TRequest> context, 
        List<IActorRef<TActor, TRequest>> instances
    )
    {
        if (instances is null || instances.Count == 0)
            throw new NixieException("A router must have at least one routee.");

        this.context = context;

        // Copied so a caller mutating its own list afterwards cannot race the router thread.
        this.instances = [.. instances];
    }

    /// <summary>
    /// Receives a message that must be routed to the least leaded routee
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public Task Receive(TRequest message)
    {
        int number = random.Next(0, instances.Count);

        // Shutdown routees are skipped in every step: an idle, empty, shutdown routee would otherwise
        // look maximally attractive while silently dropping every message sent to it.

        // Step 1. Find a router that is not processing messages
        for (int i = number; i < instances.Count; i++)
        {
            IActorRef<TActor, TRequest> instance = instances[i];

            if (instance.Runner.IsShutdown || instance.Runner.IsProcessing)
                continue;

            instance.Send(message);
            return Task.CompletedTask;
        }

        // Step 1.b. Find a router that is not processing messages
        for (int i = 0; i < number; i++)
        {
            IActorRef<TActor, TRequest> instance = instances[i];

            if (instance.Runner.IsShutdown || instance.Runner.IsProcessing)
                continue;

            instance.Send(message);
            return Task.CompletedTask;
        }

        // Step 2. Find a router where is queue is empty (next to be free)
        for (int i = number; i < instances.Count; i++)
        {
            IActorRef<TActor, TRequest> instance = instances[i];

            if (instance.Runner.IsShutdown || !instance.Runner.IsEmpty)
                continue;

            instance.Send(message);
            return Task.CompletedTask;
        }

        // Step 2.b Find a router where is queue is empty (next to be free)
        for (int i = 0; i < number; i++)
        {
            IActorRef<TActor, TRequest> instance = instances[i];

            if (instance.Runner.IsShutdown || !instance.Runner.IsEmpty)
                continue;

            instance.Send(message);
            return Task.CompletedTask;
        }

        // Step 3. Find a live router with the least number of queued messages.
        // Single O(n) pass instead of an O(n log n) LINQ sort; the strict `<` keeps the first minimum,
        // matching the stable OrderBy(...).First() tie-break. MessageCount is an approximate concurrent
        // metric, so it is read once per instance.
        IActorRef<TActor, TRequest>? leastLoaded = null;
        int leastLoadedCount = int.MaxValue;

        for (int i = 0; i < instances.Count; i++)
        {
            IActorRef<TActor, TRequest> instance = instances[i];

            if (instance.Runner.IsShutdown)
                continue;

            int count = instance.Runner.MessageCount;

            if (count < leastLoadedCount)
            {
                leastLoaded = instance;
                leastLoadedCount = count;
            }
        }

        if (leastLoaded is null)
            throw new NixieException("All routees of the balancing router are shutdown.");

        leastLoaded.Send(message);

        return Task.CompletedTask;
    }
}