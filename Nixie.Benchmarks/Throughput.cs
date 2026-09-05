using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using Nixie.Routers;

namespace Nixie.Benchmarks;

#pragma warning disable CS0169 // padding fields are never read on purpose

/// <summary>
/// Counts the messages that one actor instance processed.
/// Every actor owns one counter. The padding fields keep two counters on separate cache lines,
/// so a router with many routees does not measure false sharing instead of message throughput.
/// </summary>
public sealed class Counter
{
    private long pad0, pad1, pad2, pad3, pad4, pad5, pad6;

    public long Value;

    private long pad7, pad8, pad9, pad10, pad11, pad12, pad13;

    public Counter() => Tally.Register(this);
}

/// <summary>
/// Registry of every counter created in the process. The harness resets the registry before a
/// measurement. It sums the registry after the drain to prove that the actors processed every message.
/// </summary>
public static class Tally
{
    private static readonly ConcurrentBag<Counter> Counters = [];

    public static void Register(Counter counter) => Counters.Add(counter);

    public static void Reset()
    {
        foreach (Counter counter in Counters)
            counter.Value = 0;
    }

    public static long Total()
    {
        long total = 0;

        foreach (Counter counter in Counters)
            total += counter.Value;

        return total;
    }
}

/// <summary>
/// Reference-type request. The hash selects a routee in the consistent-hash router.
/// </summary>
public sealed class Ping : IConsistentHashable
{
    public int Key;

    public int GetHash() => Key;
}

/// <summary>
/// Value-type request for the struct actors and the struct routers.
/// </summary>
public readonly struct PingStruct : IConsistentHashable
{
    private readonly int key;

    public PingStruct(int key) => this.key = key;

    public int GetHash() => key;
}

/// <summary>
/// Reference-type response.
/// </summary>
public sealed class Pong
{
}

/// <summary>
/// Value-type response. A struct actor must reply with a struct.
/// </summary>
public readonly struct PongStruct
{
}

/// <summary>
/// Fire-and-forget actor with a reference-type request.
/// </summary>
public sealed class ThroughputActor : IActor<Ping>
{
    private readonly Counter counter = new();

    public ThroughputActor(IActorContext<ThroughputActor, Ping> _) { }

    public Task Receive(Ping message)
    {
        counter.Value++;
        return Task.CompletedTask;
    }
}

/// <summary>
/// Fire-and-forget actor with a value-type request.
/// </summary>
public sealed class ThroughputStructActor : IActorStruct<PingStruct>
{
    private readonly Counter counter = new();

    public ThroughputStructActor(IActorContextStruct<ThroughputStructActor, PingStruct> _) { }

    public Task Receive(PingStruct message)
    {
        counter.Value++;
        return Task.CompletedTask;
    }
}

/// <summary>
/// Fire-and-forget actor that receives a batch of messages per call.
/// </summary>
public sealed class ThroughputAggregateActor : IActorAggregate<Ping>
{
    private readonly Counter counter = new();

    public ThroughputAggregateActor(IActorAggregateContext<ThroughputAggregateActor, Ping> _) { }

    public Task Receive(List<Ping> messages)
    {
        counter.Value += messages.Count;
        return Task.CompletedTask;
    }
}

/// <summary>
/// Reply actor with a reference-type request. The reply task is cached, so the handler allocates nothing.
/// </summary>
public sealed class ThroughputReplyActor : IActor<Ping, Pong>
{
    private static readonly Task<Pong?> Response = Task.FromResult<Pong?>(new Pong());

    private readonly Counter counter = new();

    public ThroughputReplyActor(IActorContext<ThroughputReplyActor, Ping, Pong> _) { }

    public Task<Pong?> Receive(Ping message)
    {
        counter.Value++;
        return Response;
    }
}

/// <summary>
/// Reply actor with a value-type request and a value-type response.
/// </summary>
public sealed class ThroughputStructReplyActor : IActorStruct<PingStruct, PongStruct>
{
    private static readonly Task<PongStruct> Response = Task.FromResult(new PongStruct());

    private readonly Counter counter = new();

    public ThroughputStructReplyActor(IActorContextStruct<ThroughputStructReplyActor, PingStruct, PongStruct> _) { }

    public Task<PongStruct> Receive(PingStruct message)
    {
        counter.Value++;
        return Response;
    }
}

/// <summary>
/// Reply actor that receives a batch of messages per call. This actor completes each promise itself.
/// </summary>
public sealed class ThroughputAggregateReplyActor : IActorAggregate<Ping, Pong>
{
    private static readonly Pong Response = new();

    private readonly Counter counter = new();

    public ThroughputAggregateReplyActor(IActorAggregateContext<ThroughputAggregateReplyActor, Ping, Pong> _) { }

    public Task Receive(List<ActorMessageReply<Ping, Pong>> messages)
    {
        counter.Value += messages.Count;

        for (int i = 0; i < messages.Count; i++)
        {
            ActorMessageReply<Ping, Pong> message = messages[i];
            message.Promise?.TrySetResult(Response);
        }

        return Task.CompletedTask;
    }
}

/// <summary>
/// One measured throughput scenario.
/// </summary>
public readonly struct ThroughputRow
{
    public string Group { get; init; }

    public string Name { get; init; }

    public int Producers { get; init; }

    public long Messages { get; init; }

    public double Seconds { get; init; }

    public long Processed { get; init; }

    public double MessagesPerSecond => Messages / Seconds;

    public double NanosecondsPerMessage => Seconds * 1_000_000_000d / Messages;
}

/// <summary>
/// Throughput benchmark: how many messages per second one actor, or one router, processes.
/// </summary>
public static class Throughput
{
    private const int Warmup = 20_000;

    private const int KeyCount = 1024;

    private static readonly List<ThroughputRow> Rows = [];

    public static async Task Run(string[] args)
    {
        // dotnet run -c Release -- throughput <floodN> <askN> <routees>
        int floodN = Arg(args, 1, 2_000_000);
        int askN = Arg(args, 2, 100_000);
        int routees = Arg(args, 3, 4);
        int producers = Environment.ProcessorCount;

        Ping[] keys = new Ping[KeyCount];
        PingStruct[] structKeys = new PingStruct[KeyCount];

        for (int i = 0; i < KeyCount; i++)
        {
            keys[i] = new() { Key = i };
            structKeys[i] = new(i);
        }

        Console.WriteLine("Nixie throughput benchmark");
        Console.WriteLine("Runtime: {0}, {1}, {2} logical cores", Environment.Version,
            System.Runtime.InteropServices.RuntimeInformation.OSArchitecture, Environment.ProcessorCount);
        Console.WriteLine("Server GC: {0}, Latency mode: {1}", System.Runtime.GCSettings.IsServerGC,
            System.Runtime.GCSettings.LatencyMode);
        Console.WriteLine("Flood messages: {0:N0}. Ask messages: {1:N0}. Routees per router: {2}.", floodN, askN, routees);
        Console.WriteLine();

        await FireAndForget(floodN, keys, structKeys);
        await Routers(floodN, routees, keys, structKeys);
        await MultipleProducers(floodN, producers, routees, keys);
        await RoundTrips(askN, routees, keys, structKeys);

        Report();
    }

    private static int Arg(string[] args, int index, int fallback)
        => args.Length > index ? int.Parse(args[index], CultureInfo.InvariantCulture) : fallback;

    // ---------------------------------------------------------------- scenarios

    private static async Task FireAndForget(int n, Ping[] keys, PingStruct[] structKeys)
    {
        {
            using ActorSystem system = new();
            IActorRef<ThroughputActor, Ping> actor = system.Spawn<ThroughputActor, Ping>("send");
            await Flood("Single actor, class message, Send", "actor types", n, 1, (from, count) =>
            {
                for (int i = from; i < from + count; i++)
                    actor.Send(keys[i & (KeyCount - 1)]);
            }, system);
        }

        {
            using ActorSystem system = new();
            IActorRefStruct<ThroughputStructActor, PingStruct> actor = system.SpawnStruct<ThroughputStructActor, PingStruct>("sendstruct");
            await Flood("Single actor, struct message, Send", "actor types", n, 1, (from, count) =>
            {
                for (int i = from; i < from + count; i++)
                    actor.Send(structKeys[i & (KeyCount - 1)]);
            }, system);
        }

        {
            using ActorSystem system = new();
            IActorRefAggregate<ThroughputAggregateActor, Ping> actor = system.SpawnAggregate<ThroughputAggregateActor, Ping>("sendaggregate");
            await Flood("Single actor, aggregate batch, Send", "actor types", n, 1, (from, count) =>
            {
                for (int i = from; i < from + count; i++)
                    actor.Send(keys[i & (KeyCount - 1)]);
            }, system);
        }

        {
            using ActorSystem system = new();
            IActorRef<ThroughputReplyActor, Ping, Pong> actor = system.Spawn<ThroughputReplyActor, Ping, Pong>("trysendreply");
            await Flood("Single reply actor, TrySend, no reply read", "actor types", n, 1, (from, count) =>
            {
                for (int i = from; i < from + count; i++)
                    actor.TrySend(keys[i & (KeyCount - 1)]);
            }, system);
        }
    }

    private static async Task Routers(int n, int routees, Ping[] keys, PingStruct[] structKeys)
    {
        string suffix = " (" + routees + " routees)";

        {
            using ActorSystem system = new();
            IActorRef<RoundRobinActor<ThroughputActor, Ping>, Ping> router =
                system.CreateRoundRobinRouter<ThroughputActor, Ping>("rr", routees);

            await Flood("Round-robin router, class message" + suffix, "routers", n, 1, (from, count) =>
            {
                for (int i = from; i < from + count; i++)
                    router.Send(keys[i & (KeyCount - 1)]);
            }, system);
        }

        {
            using ActorSystem system = new();
            IActorRef<ConsistentHashActor<ThroughputActor, Ping>, Ping> router =
                system.CreateConsistentHashRouter<ThroughputActor, Ping>("chash", routees);

            await Flood("Consistent-hash router, class message" + suffix, "routers", n, 1, (from, count) =>
            {
                for (int i = from; i < from + count; i++)
                    router.Send(keys[i & (KeyCount - 1)]);
            }, system);
        }

        {
            using ActorSystem system = new();
            IActorRef<BalancingActor<ThroughputActor, Ping>, Ping> router =
                system.CreateBalancingRouter<ThroughputActor, Ping>("balancing", routees);

            await Flood("Balancing router, class message" + suffix, "routers", n, 1, (from, count) =>
            {
                for (int i = from; i < from + count; i++)
                    router.Send(keys[i & (KeyCount - 1)]);
            }, system);
        }

        {
            using ActorSystem system = new();
            IActorRefStruct<RoundRobinActorStruct<ThroughputStructActor, PingStruct>, PingStruct> router =
                system.CreateRoundRobinRouterStruct<ThroughputStructActor, PingStruct>("rrstruct", routees);

            await Flood("Round-robin router, struct message" + suffix, "routers", n, 1, (from, count) =>
            {
                for (int i = from; i < from + count; i++)
                    router.Send(structKeys[i & (KeyCount - 1)]);
            }, system);
        }

        {
            using ActorSystem system = new();
            IActorRefStruct<ConsistentHashActorStruct<ThroughputStructActor, PingStruct>, PingStruct> router =
                system.CreateConsistentHashRouterStruct<ThroughputStructActor, PingStruct>("chashstruct", routees);

            await Flood("Consistent-hash router, struct message" + suffix, "routers", n, 1, (from, count) =>
            {
                for (int i = from; i < from + count; i++)
                    router.Send(structKeys[i & (KeyCount - 1)]);
            }, system);
        }
    }

    private static async Task MultipleProducers(int n, int producers, int routees, Ping[] keys)
    {
        string suffix = " (" + producers + " producer threads)";

        {
            using ActorSystem system = new();
            IActorRef<ThroughputActor, Ping> actor = system.Spawn<ThroughputActor, Ping>("mpsend");

            await Flood("Single actor, class message, Send" + suffix, "many producers", n, producers, (from, count) =>
            {
                for (int i = from; i < from + count; i++)
                    actor.Send(keys[i & (KeyCount - 1)]);
            }, system);
        }

        {
            using ActorSystem system = new();
            IActorRef<RoundRobinActor<ThroughputActor, Ping>, Ping> router =
                system.CreateRoundRobinRouter<ThroughputActor, Ping>("mprr", routees);

            await Flood("Round-robin router, " + routees + " routees" + suffix, "many producers", n, producers, (from, count) =>
            {
                for (int i = from; i < from + count; i++)
                    router.Send(keys[i & (KeyCount - 1)]);
            }, system);
        }

        {
            using ActorSystem system = new();
            IActorRef<ConsistentHashActor<ThroughputActor, Ping>, Ping> router =
                system.CreateConsistentHashRouter<ThroughputActor, Ping>("mpchash", routees);

            await Flood("Consistent-hash router, " + routees + " routees" + suffix, "many producers", n, producers, (from, count) =>
            {
                for (int i = from; i < from + count; i++)
                    router.Send(keys[i & (KeyCount - 1)]);
            }, system);
        }
    }

    private static async Task RoundTrips(int n, int routees, Ping[] keys, PingStruct[] structKeys)
    {
        {
            using ActorSystem system = new();
            IActorRef<ThroughputReplyActor, Ping, Pong> actor = system.Spawn<ThroughputReplyActor, Ping, Pong>("ask");
            await Ask("Class actor, Ask, one at a time", n, 1, i => actor.Ask(keys[i & (KeyCount - 1)]));
        }

        {
            using ActorSystem system = new();
            IActorRef<ThroughputReplyActor, Ping, Pong> actor = system.Spawn<ThroughputReplyActor, Ping, Pong>("askpipelined");
            await Ask("Class actor, Ask, 64 in flight", n, 64, i => actor.Ask(keys[i & (KeyCount - 1)]));
        }

        {
            using ActorSystem system = new();
            IActorRef<ThroughputReplyActor, Ping, Pong> actor = system.Spawn<ThroughputReplyActor, Ping, Pong>("askpooled");

            await Ask("Class actor, TryAskPooled, one at a time", n, 1, i =>
            {
                if (!actor.TryAskPooled(keys[i & (KeyCount - 1)], out ValueTask<Pong?> reply))
                    throw new InvalidOperationException("the ask was rejected");

                return reply.AsTask();
            });
        }

        {
            using ActorSystem system = new();
            IActorRefStruct<ThroughputStructReplyActor, PingStruct, PongStruct> actor =
                system.SpawnStruct<ThroughputStructReplyActor, PingStruct, PongStruct>("askstruct");

            await Ask("Struct actor, Ask, one at a time", n, 1, async i =>
            {
                await actor.Ask(structKeys[i & (KeyCount - 1)]);
            });
        }

        {
            using ActorSystem system = new();
            IActorRefAggregate<ThroughputAggregateReplyActor, Ping, Pong> actor =
                system.SpawnAggregate<ThroughputAggregateReplyActor, Ping, Pong>("askaggregate");

            await Ask("Aggregate actor, Ask, 64 in flight", n, 64, i => actor.Ask(keys[i & (KeyCount - 1)]));
        }

        {
            using ActorSystem system = new();
            IActorRef<RoundRobinActor<ThroughputReplyActor, Ping, Pong>, Ping, Pong> router =
                system.CreateRoundRobinRouter<ThroughputReplyActor, Ping, Pong>("rrask", routees);

            await Ask("Round-robin router, Ask, 64 in flight (" + routees + " routees)", n, 64,
                i => router.Ask(keys[i & (KeyCount - 1)]));
        }
    }

    // ---------------------------------------------------------------- harness

    /// <summary>
    /// Measures a flood: the producers admit every message, then the harness waits for the drain.
    /// The elapsed time covers both phases, so the result is end-to-end throughput.
    /// </summary>
    private static async Task Flood(string name, string group, int n, int producers, Action<int, int> send, ActorSystem system)
    {
        send(0, Warmup);
        await system.Wait();

        Tally.Reset();
        GC.Collect();
        GC.WaitForPendingFinalizers();

        long start = Stopwatch.GetTimestamp();

        if (producers == 1)
        {
            send(0, n);
        }
        else
        {
            int chunk = n / producers;
            int remainder = n - (chunk * producers);
            Thread[] threads = new Thread[producers];

            for (int p = 0; p < producers; p++)
            {
                int from = p * chunk;
                int count = p == producers - 1 ? chunk + remainder : chunk;
                threads[p] = new(() => send(from, count)) { IsBackground = true };
            }

            foreach (Thread thread in threads)
                thread.Start();

            foreach (Thread thread in threads)
                thread.Join();
        }

        await system.Wait();

        double seconds = Stopwatch.GetElapsedTime(start).TotalSeconds;

        Rows.Add(new()
        {
            Group = group,
            Name = name,
            Producers = producers,
            Messages = n,
            Seconds = seconds,
            Processed = Tally.Total(),
        });
    }

    /// <summary>
    /// Measures request and response round trips. <paramref name="inFlight"/> is the batch size:
    /// the harness issues that many asks, then awaits all of them before it issues the next batch.
    /// </summary>
    private static async Task Ask(string name, int n, int inFlight, Func<int, Task> ask)
    {
        for (int i = 0; i < Warmup; i++)
            await ask(i);

        Tally.Reset();
        GC.Collect();
        GC.WaitForPendingFinalizers();

        long start = Stopwatch.GetTimestamp();

        if (inFlight == 1)
        {
            for (int i = 0; i < n; i++)
                await ask(i);
        }
        else
        {
            Task[] batch = new Task[inFlight];

            for (int i = 0; i < n; i += inFlight)
            {
                int size = Math.Min(inFlight, n - i);

                for (int j = 0; j < size; j++)
                    batch[j] = ask(i + j);

                for (int j = 0; j < size; j++)
                    await batch[j];
            }
        }

        double seconds = Stopwatch.GetElapsedTime(start).TotalSeconds;

        Rows.Add(new()
        {
            Group = "round trips",
            Name = name,
            Producers = 1,
            Messages = n,
            Seconds = seconds,
            Processed = Tally.Total(),
        });
    }

    // ---------------------------------------------------------------- report

    private static void Report()
    {
        Console.WriteLine();
        Console.WriteLine("{0,-56} {1,4} {2,12} {3,16} {4,10}", "Scenario", "prod", "messages", "messages/s", "ns/msg");
        Console.WriteLine(new string('-', 104));

        string? group = null;

        foreach (ThroughputRow row in Rows)
        {
            if (row.Group != group)
            {
                group = row.Group;
                Console.WriteLine();
                Console.WriteLine("  {0}", group.ToUpperInvariant());
            }

            string flag = row.Processed == row.Messages ? "" : "  [processed " + row.Processed.ToString("N0", CultureInfo.InvariantCulture) + "]";

            Console.WriteLine("{0,-56} {1,4} {2,12:N0} {3,16:N0} {4,10:N1}{5}",
                row.Name, row.Producers, row.Messages, row.MessagesPerSecond, row.NanosecondsPerMessage, flag);
        }

        Console.WriteLine();
        Console.WriteLine("How to read this");
        Console.WriteLine(new string('-', 104));
        Console.WriteLine("  A flood row admits every message first, then waits for the drain. The inbox therefore");
        Console.WriteLine("  grows large. The number is the end-to-end rate, not a steady-state rate under a paced load.");
        Console.WriteLine();
        Console.WriteLine("  A round-trip row is a different workload. One at a time means the actor never batches its");
        Console.WriteLine("  wakeups, so that rate is a latency measurement, not a throughput ceiling.");
        Console.WriteLine();
        Console.WriteLine("  A router adds one actor hop: the router actor receives the message, then forwards it.");
        Console.WriteLine("  Compare a router row with the single-actor row in the first group to see that cost.");
        Console.WriteLine();
        Console.WriteLine("  A single trial is reported per scenario. Repeat the run before you treat a small");
        Console.WriteLine("  difference as real.");
    }
}
