using System.Diagnostics;
using System.Globalization;
using Nixie;

namespace Nixie.Benchmarks;

/// <summary>
/// Reply message. A single cached instance is reused for every send so the message allocation itself does not
/// distort the measurement.
/// </summary>
public sealed class Payload
{
    public int Value;
}

/// <summary>
/// Realistic reply actor: it caches the response object, but it still builds a task for every reply.
/// Task.FromResult of a non-null reference allocates on .NET 8, so this handler carries a cost of its own.
/// Compare it with <see cref="BenchCachedTaskReplyActor"/> to separate handler cost from framework cost.
/// </summary>
public sealed class BenchReplyActor : IActor<Payload, Payload>
{
    private static readonly Payload Response = new();

    public BenchReplyActor(IActorContext<BenchReplyActor, Payload, Payload> _) { }

    public Task<Payload?> Receive(Payload message) => Task.FromResult<Payload?>(Response);
}

/// <summary>
/// Framework-floor reply actor: it caches the reply task itself, so the handler allocates nothing and the
/// measurement shows the cost of the messaging path alone.
/// </summary>
public sealed class BenchCachedTaskReplyActor : IActor<Payload, Payload>
{
    private static readonly Task<Payload?> Response = Task.FromResult<Payload?>(new Payload());

    public BenchCachedTaskReplyActor(IActorContext<BenchCachedTaskReplyActor, Payload, Payload> _) { }

    public Task<Payload?> Receive(Payload message) => Response;
}

/// <summary>
/// One measured scenario: bytes allocated per operation and elapsed time per operation.
/// </summary>
public readonly struct Measurement
{
    public double BytesPerOp { get; init; }

    public double NanosecondsPerOp { get; init; }

    public int Operations { get; init; }
}

public static class Program
{
    private const int WarmupOperations = 5_000;

    public static async Task Main(string[] args)
    {
        // Iteration counts (override: dotnet run -c Release -- <askN> <trySendN> <rawTcsN>).
        int askN = args.Length > 0 ? int.Parse(args[0], CultureInfo.InvariantCulture) : 100_000;
        int trySendN = args.Length > 1 ? int.Parse(args[1], CultureInfo.InvariantCulture) : 200_000;
        int rawTcsN = args.Length > 2 ? int.Parse(args[2], CultureInfo.InvariantCulture) : 1_000_000;

        Console.WriteLine("Nixie messaging allocation benchmark");
        Console.WriteLine("Runtime: {0}, {1}", Environment.Version, System.Runtime.InteropServices.RuntimeInformation.OSArchitecture);
        Console.WriteLine("Server GC: {0}, Latency mode: {1}", System.Runtime.GCSettings.IsServerGC, System.Runtime.GCSettings.LatencyMode);
        Console.WriteLine("Warmup per scenario: {0:N0} operations", WarmupOperations);
        Console.WriteLine("Allocation counter: GC.GetTotalAllocatedBytes(precise), which is process-wide and");
        Console.WriteLine("therefore includes the drain work done on thread-pool threads.");
        Console.WriteLine();

        Payload msg = new();

        // Serial round trips. Each one waits for its own reply, so the actor never batches its wakeups.
        Measurement ask = await MeasureSerial("ask", askN, static (actor, message) => actor.Ask(message));

        Measurement tryAsk = await MeasureSerial("tryask", askN, static (actor, message) =>
        {
            if (!actor.TryAsk(message, out Task<Payload?>? reply))
                throw new InvalidOperationException("the ask was rejected");

            return reply;
        });

        Measurement tryAskPooled = await MeasureSerialPooled(askN);

        Measurement askRealistic = await MeasureSerialRealistic(askN);

        // Floods. Every message is admitted before any reply is awaited, so these include the cost of
        // inbox growth and cannot be compared operation for operation with the serial numbers above.
        Measurement send = await MeasureFlood("send", trySendN, static (actor, message) => actor.Send(message));
        Measurement trySend = await MeasureFlood("trysend", trySendN, static (actor, message) => actor.TrySend(message));

        double rawTcsObjectPerOp = MeasureRawTcsObject(rawTcsN);

        Console.WriteLine();
        Console.WriteLine("{0,-46} {1,10} {2,12} {3,12}", "Scenario", "ops", "bytes/op", "ns/op");
        Console.WriteLine(new string('-', 84));
        Report("Ask, cached reply task (serial)", ask);
        Report("TryAsk, cached reply task (serial)", tryAsk);
        Report("TryAskPooled, cached reply task (serial)", tryAskPooled);
        Report("Ask, handler builds its reply task (serial)", askRealistic);
        Report("Send, promise-free (flood)", send);
        Report("TrySend, promise-free (flood)", trySend);
        Console.WriteLine("{0,-46} {1,10:N0} {2,12:N1} {3,12}", "Raw TaskCompletionSource<T> object", rawTcsN, rawTcsObjectPerOp, "-");
        Console.WriteLine();

        Console.WriteLine("How to read this");
        Console.WriteLine(new string('-', 84));
        Console.WriteLine("  A serial ask and a flooded send are different workloads: the flood grows the inbox in");
        Console.WriteLine("  large steps and wakes the drainer once for many messages, so subtracting one from the");
        Console.WriteLine("  other does not measure the reply promise. Compare Ask with TryAsk and TryAskPooled,");
        Console.WriteLine("  which share the same actor, the same concurrency and the same drain pattern.");
        Console.WriteLine();
        Console.WriteLine("  The two Ask rows differ only in the handler: one returns a cached task and one builds");
        Console.WriteLine("  a task per reply. Their difference is the handler's own cost, not the framework's.");
        Console.WriteLine();
        Console.WriteLine("  A single trial is reported per scenario. Repeat the run before you treat any");
        Console.WriteLine("  difference smaller than the run-to-run spread as real, and measure throughput and");
        Console.WriteLine("  latency at the concurrency your application actually uses.");
    }

    private static void Report(string name, Measurement measurement)
    {
        Console.WriteLine("{0,-46} {1,10:N0} {2,12:N1} {3,12:N0}", name, measurement.Operations, measurement.BytesPerOp, measurement.NanosecondsPerOp);
    }

    private static async Task<Measurement> MeasureSerial(
        string name,
        int n,
        Func<IActorRef<BenchCachedTaskReplyActor, Payload, Payload>, Payload, Task<Payload?>> ask)
    {
        using ActorSystem system = new();
        IActorRef<BenchCachedTaskReplyActor, Payload, Payload> actor =
            system.Spawn<BenchCachedTaskReplyActor, Payload, Payload>(name);

        Payload msg = new();

        for (int i = 0; i < WarmupOperations; i++)
            await ask(actor, msg);

        GC.Collect();
        GC.WaitForPendingFinalizers();

        long before = GC.GetTotalAllocatedBytes(precise: true);
        long start = Stopwatch.GetTimestamp();

        for (int i = 0; i < n; i++)
            await ask(actor, msg);

        double elapsed = Stopwatch.GetElapsedTime(start).TotalNanoseconds;
        long after = GC.GetTotalAllocatedBytes(precise: true);

        return new() { BytesPerOp = (after - before) / (double)n, NanosecondsPerOp = elapsed / n, Operations = n };
    }

    private static async Task<Measurement> MeasureSerialPooled(int n)
    {
        using ActorSystem system = new();
        IActorRef<BenchCachedTaskReplyActor, Payload, Payload> actor =
            system.Spawn<BenchCachedTaskReplyActor, Payload, Payload>("tryaskpooled");

        Payload msg = new();

        for (int i = 0; i < WarmupOperations; i++)
        {
            if (!actor.TryAskPooled(msg, out ValueTask<Payload?> warm))
                throw new InvalidOperationException("the ask was rejected");

            await warm;
        }

        GC.Collect();
        GC.WaitForPendingFinalizers();

        long before = GC.GetTotalAllocatedBytes(precise: true);
        long start = Stopwatch.GetTimestamp();

        for (int i = 0; i < n; i++)
        {
            if (!actor.TryAskPooled(msg, out ValueTask<Payload?> reply))
                throw new InvalidOperationException("the ask was rejected");

            await reply;
        }

        double elapsed = Stopwatch.GetElapsedTime(start).TotalNanoseconds;
        long after = GC.GetTotalAllocatedBytes(precise: true);

        return new() { BytesPerOp = (after - before) / (double)n, NanosecondsPerOp = elapsed / n, Operations = n };
    }

    private static async Task<Measurement> MeasureSerialRealistic(int n)
    {
        using ActorSystem system = new();
        IActorRef<BenchReplyActor, Payload, Payload> actor =
            system.Spawn<BenchReplyActor, Payload, Payload>("ask-realistic");

        Payload msg = new();

        for (int i = 0; i < WarmupOperations; i++)
            await actor.Ask(msg);

        GC.Collect();
        GC.WaitForPendingFinalizers();

        long before = GC.GetTotalAllocatedBytes(precise: true);
        long start = Stopwatch.GetTimestamp();

        for (int i = 0; i < n; i++)
            await actor.Ask(msg);

        double elapsed = Stopwatch.GetElapsedTime(start).TotalNanoseconds;
        long after = GC.GetTotalAllocatedBytes(precise: true);

        return new() { BytesPerOp = (after - before) / (double)n, NanosecondsPerOp = elapsed / n, Operations = n };
    }

    private static async Task<Measurement> MeasureFlood(
        string name,
        int n,
        Action<IActorRef<BenchCachedTaskReplyActor, Payload, Payload>, Payload> send)
    {
        using ActorSystem system = new();
        IActorRef<BenchCachedTaskReplyActor, Payload, Payload> actor =
            system.Spawn<BenchCachedTaskReplyActor, Payload, Payload>(name);

        Payload msg = new();

        for (int i = 0; i < WarmupOperations; i++)
            send(actor, msg);

        await system.Wait();

        GC.Collect();
        GC.WaitForPendingFinalizers();

        long before = GC.GetTotalAllocatedBytes(precise: true);
        long start = Stopwatch.GetTimestamp();

        for (int i = 0; i < n; i++)
            send(actor, msg);

        // The drain is part of the work: it is included in both counters.
        await system.Wait();

        double elapsed = Stopwatch.GetElapsedTime(start).TotalNanoseconds;
        long after = GC.GetTotalAllocatedBytes(precise: true);

        return new() { BytesPerOp = (after - before) / (double)n, NanosecondsPerOp = elapsed / n, Operations = n };
    }

    private static double MeasureRawTcsObject(int n)
    {
        // Isolates the TaskCompletionSource<T> and its Task object: the objects a pooled reply removes.
        // No await here, so this is an object-allocation cost only, not a round trip.
        Payload cached = new();

        for (int i = 0; i < 10_000; i++)
        {
            TaskCompletionSource<Payload?> warm = new(TaskCreationOptions.RunContinuationsAsynchronously);
            warm.SetResult(cached);
            _ = warm.Task.IsCompletedSuccessfully;
        }

        GC.Collect();
        GC.WaitForPendingFinalizers();

        long before = GC.GetTotalAllocatedBytes(precise: true);
        for (int i = 0; i < n; i++)
        {
            TaskCompletionSource<Payload?> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
            tcs.SetResult(cached);
            _ = tcs.Task.IsCompletedSuccessfully;
        }
        long after = GC.GetTotalAllocatedBytes(precise: true);

        return (after - before) / (double)n;
    }
}
