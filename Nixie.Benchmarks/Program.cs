
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
/// Minimal reply actor: returns a cached response. The <see cref="Task.FromResult{TResult}(TResult)"/> it
/// creates is present on both the Ask and the TrySend paths, so it cancels out in their difference.
/// </summary>
public sealed class BenchReplyActor : IActor<Payload, Payload>
{
    private static readonly Payload Response = new();

    public BenchReplyActor(IActorContext<BenchReplyActor, Payload, Payload> _) { }

    public Task<Payload?> Receive(Payload message) => Task.FromResult<Payload?>(Response);
}

public static class Program
{
    public static async Task Main(string[] args)
    {
        // Iteration counts (override: dotnet run -c Release -- <askN> <trySendN> <rawTcsN>).
        int askN = args.Length > 0 ? int.Parse(args[0], CultureInfo.InvariantCulture) : 100_000;
        int trySendN = args.Length > 1 ? int.Parse(args[1], CultureInfo.InvariantCulture) : 200_000;
        int rawTcsN = args.Length > 2 ? int.Parse(args[2], CultureInfo.InvariantCulture) : 1_000_000;

        Console.WriteLine("Nixie Ask hot-path allocation benchmark");
        Console.WriteLine("Server GC: {0}, Concurrent GC: {1}", System.Runtime.GCSettings.IsServerGC,
            System.Runtime.GCSettings.LatencyMode);
        Console.WriteLine();

        Payload msg = new();

        double askPerOp = await MeasureAsk(msg, askN);
        double trySendPerOp = await MeasureTrySend(msg, trySendN);
        double sendPerOp = await MeasureSend(msg, trySendN);
        double rawTcsObjectPerOp = MeasureRawTcsObject(rawTcsN);

        Console.WriteLine();
        Console.WriteLine("{0,-42} {1,12}", "Scenario", "bytes/op");
        Console.WriteLine(new string('-', 56));
        Console.WriteLine("{0,-42} {1,12:N1}", "Ask round-trip (full, serial)", askPerOp);
        Console.WriteLine("{0,-42} {1,12:N1}", "Send (promise-free, void)", sendPerOp);
        Console.WriteLine("{0,-42} {1,12:N1}", "TrySend (promise-free floor)", trySendPerOp);
        Console.WriteLine("{0,-42} {1,12:N1}", "Raw TaskCompletionSource<T> object", rawTcsObjectPerOp);
        Console.WriteLine();

        double promiseCostUpperBound = askPerOp - trySendPerOp;

        Console.WriteLine("Interpretation");
        Console.WriteLine(new string('-', 56));
        Console.WriteLine(
            "  Ask - TrySend               = {0,10:N1} bytes/op  (upper bound on the promise's",
            promiseCostUpperBound);
        Console.WriteLine(
            "                                             cost; includes per-op drainer");
        Console.WriteLine(
            "                                             activation differences, so it over-counts)");
        Console.WriteLine(
            "  Raw TCS<T>+Task object      = {0,10:N1} bytes/op  <-- authoritative ceiling a pool",
            rawTcsObjectPerOp);
        Console.WriteLine(
            "                                             can remove per Ask (the object itself)");
        Console.WriteLine();
        Console.WriteLine(
            "  A pooled IValueTaskSource replaces the TCS+Task object (~{0:N0} B) but does NOT remove",
            rawTcsObjectPerOp);
        Console.WriteLine(
            "  the caller's cross-thread await continuation. Realistic saving per Ask therefore");
        Console.WriteLine(
            "  approaches {0:N0} B, i.e. ~{1:N1}% of the current {2:N0} B/op Ask cost.",
            rawTcsObjectPerOp, 100.0 * rawTcsObjectPerOp / askPerOp, askPerOp);
    }

    private static async Task<double> MeasureAsk(Payload msg, int n)
    {
        using ActorSystem system = new();
        IActorRef<BenchReplyActor, Payload, Payload> actor =
            system.Spawn<BenchReplyActor, Payload, Payload>("ask");

        // Warmup: JIT the path and settle the thread pool.
        for (int i = 0; i < 5_000; i++)
            await actor.Ask(msg);

        GC.Collect();
        GC.WaitForPendingFinalizers();

        long before = GC.GetTotalAllocatedBytes(precise: true);
        for (int i = 0; i < n; i++)
            await actor.Ask(msg);
        long after = GC.GetTotalAllocatedBytes(precise: true);

        return (after - before) / (double)n;
    }

    private static async Task<double> MeasureTrySend(Payload msg, int n)
    {
        using ActorSystem system = new();
        IActorRef<BenchReplyActor, Payload, Payload> actor =
            system.Spawn<BenchReplyActor, Payload, Payload>("trysend");

        for (int i = 0; i < 5_000; i++)
            actor.TrySend(msg);
        await system.Wait();

        GC.Collect();
        GC.WaitForPendingFinalizers();

        long before = GC.GetTotalAllocatedBytes(precise: true);
        for (int i = 0; i < n; i++)
            actor.TrySend(msg);
        await system.Wait();   // O(1) relative to n; drains the flood
        long after = GC.GetTotalAllocatedBytes(precise: true);

        return (after - before) / (double)n;
    }

    private static async Task<double> MeasureSend(Payload msg, int n)
    {
        using ActorSystem system = new();
        IActorRef<BenchReplyActor, Payload, Payload> actor =
            system.Spawn<BenchReplyActor, Payload, Payload>("send");

        for (int i = 0; i < 5_000; i++)
            actor.Send(msg);
        await system.Wait();

        GC.Collect();
        GC.WaitForPendingFinalizers();

        long before = GC.GetTotalAllocatedBytes(precise: true);
        for (int i = 0; i < n; i++)
            actor.Send(msg);
        await system.Wait();
        long after = GC.GetTotalAllocatedBytes(precise: true);

        return (after - before) / (double)n;
    }

    private static double MeasureRawTcsObject(int n)
    {
        // Isolates the TaskCompletionSource<T> + its Task object cost — exactly what a pool eliminates.
        // No await: RunContinuationsAsynchronously + a completed task would not model the real cross-thread
        // continuation anyway, so we measure only the object allocation here.
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
