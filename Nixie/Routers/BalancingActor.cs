
namespace Nixie.Routers;

public class BalancingActor<TActor, TRequest> : IActor<TRequest>
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
        this.context = context;

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
        this.context = context;
        this.instances = instances;
    }

    /// <summary>
    /// Receives a message that must be routed to the least leaded routee
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public Task Receive(TRequest message)
    {
        int number = random.Next(0, instances.Count);
        
        // Step 1. Find a router that is not processing messages
        for (int i = number; i < instances.Count; i++)
        {
            IActorRef<TActor, TRequest> instance = instances[i];
            
            if (instance.Runner.IsProcessing)
                continue;
            
            instance.Send(message);
            return Task.CompletedTask;
        }
        
        // Step 1.b. Find a router that is not processing messages
        for (int i = 0; i < number; i++)
        {
            IActorRef<TActor, TRequest> instance = instances[i];
            
            if (instance.Runner.IsProcessing)
                continue;
            
            instance.Send(message);
            return Task.CompletedTask;
        }
        
        // Step 2. Find a router where is queue is empty (next to be free)
        for (int i = number; i < instances.Count; i++)
        {
            IActorRef<TActor, TRequest> instance = instances[i];
            
            if (!instance.Runner.IsEmpty)
                continue;
            
            instance.Send(message);
            return Task.CompletedTask;
        }
        
        // Step 2.b Find a router where is queue is empty (next to be free)
        for (int i = 0; i < number; i++)
        {
            IActorRef<TActor, TRequest> instance = instances[i];
            
            if (!instance.Runner.IsEmpty)
                continue;
            
            instance.Send(message);
            return Task.CompletedTask;
        }
        
        // Step 3. Find a router with the least number of queued messages.
        // Single O(n) pass instead of an O(n log n) LINQ sort; the strict `<` keeps the first minimum,
        // matching the stable OrderBy(...).First() tie-break. MessageCount is an approximate concurrent
        // metric, so it is read once per instance.
        IActorRef<TActor, TRequest> leastLoaded = instances[0];
        int leastLoadedCount = leastLoaded.Runner.MessageCount;

        for (int i = 1; i < instances.Count; i++)
        {
            IActorRef<TActor, TRequest> instance = instances[i];
            int count = instance.Runner.MessageCount;

            if (count < leastLoadedCount)
            {
                leastLoaded = instance;
                leastLoadedCount = count;
            }
        }

        leastLoaded.Send(message);

        return Task.CompletedTask;
    }
}