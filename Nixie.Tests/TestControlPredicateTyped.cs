
using Nixie.Tests.Actors;

namespace Nixie.Tests;

[Collection("Nixie")]
public sealed class TestControlPredicateTyped
{
    // ---- The typed ActorRunnerOptions<TRequest> classifier is honored end-to-end ----

    [Fact]
    public async Task TestTypedControlPredicateClassifiesStructRequestsExemptAndOvertakes()
    {
        using ActorSystem asx = new();

        // Typed options: the predicate receives PriorityStructRequest directly (no boxing).
        IActorRefStruct<PriorityStructActor, PriorityStructRequest, int> actor =
            asx.SpawnStructWithOptions<PriorityStructActor, PriorityStructRequest, int>(
                "typed-struct", new ActorRunnerOptions<PriorityStructRequest>
                {
                    MaxInboxSize = 2,
                    IsControlMessage = r => r.IsControl
                });

        Task<int> n0 = actor.Ask(new PriorityStructRequest { Id = 0 });
        await Task.Delay(100);

        Task<int> n1 = actor.Ask(new PriorityStructRequest { Id = 1 });
        Task<int> n2 = actor.Ask(new PriorityStructRequest { Id = 2 });

        // Backpressure still applies to ordinary messages.
        await Assert.ThrowsAsync<ActorBusyException>(async () =>
            await actor.Ask(new PriorityStructRequest { Id = 3 }));

        // The typed predicate classifies this as control: admitted despite the full inbox, and it overtakes.
        actor.Send(new PriorityStructRequest { Id = 99, IsControl = true });

        await Task.WhenAll(n0, n1, n2);
        await asx.Wait();

        PriorityStructActor impl = (PriorityStructActor)actor.Runner.Actor!;
        int[] order = [.. impl.ProcessedOrder];

        Assert.Equal(0, order[0]);
        int ic = Array.IndexOf(order, 99);
        int i1 = Array.IndexOf(order, 1);
        int i2 = Array.IndexOf(order, 2);
        Assert.True(ic >= 0 && ic < i1 && ic < i2, "typed control predicate must classify and overtake");
        Assert.True(i1 < i2, "FIFO within the normal class");
        Assert.DoesNotContain(3, order);
    }

    [Fact]
    public async Task TestTypedAndUntypedControlPredicatesAgreeOnStructClassification()
    {
        // Both option shapes must classify identically; only their allocation profile differs.
        int[] typed = await ProcessedOrder(useTyped: true);
        int[] untyped = await ProcessedOrder(useTyped: false);

        Assert.Equal(untyped, typed);
    }

    private static async Task<int[]> ProcessedOrder(bool useTyped)
    {
        using ActorSystem asx = new();

        ActorRunnerOptions options = useTyped
            ? new ActorRunnerOptions<PriorityStructRequest> { MaxInboxSize = 2, IsControlMessage = r => r.IsControl }
            : new ActorRunnerOptions { MaxInboxSize = 2, IsControlMessage = o => ((PriorityStructRequest)o).IsControl };

        IActorRefStruct<PriorityStructActor, PriorityStructRequest, int> actor =
            asx.SpawnStructWithOptions<PriorityStructActor, PriorityStructRequest, int>(null, options);

        Task<int> n0 = actor.Ask(new PriorityStructRequest { Id = 0 });
        await Task.Delay(100);
        Task<int> n1 = actor.Ask(new PriorityStructRequest { Id = 1 });
        Task<int> n2 = actor.Ask(new PriorityStructRequest { Id = 2 });
        actor.Send(new PriorityStructRequest { Id = 99, IsControl = true });

        await Task.WhenAll(n0, n1, n2);
        await asx.Wait();

        return [.. ((PriorityStructActor)actor.Runner.Actor!).ProcessedOrder];
    }

    // ---- The typed classifier does not box the struct request; the untyped one does ----

    [Fact]
    public async Task TestTypedControlPredicateAllocatesLessThanUntyped()
    {
        const int warmup = 2_000;
        const int count = 50_000;

        // Warm both paths so JIT compilation doesn't skew the measured runs.
        await Drain(useTyped: true, warmup);
        await Drain(useTyped: false, warmup);

        long typedBytes = await MeasureDrain(useTyped: true, count);
        long untypedBytes = await MeasureDrain(useTyped: false, count);

        // Every other allocation on the send/drain path is identical between the two runs, so the difference
        // is the per-message struct box in the untyped classifier.
        Assert.True(untypedBytes > typedBytes,
            $"typed path should allocate less; typed={typedBytes} untyped={untypedBytes}");
        Assert.True(untypedBytes - typedBytes > count * 8L,
            $"expected the untyped path to box ~{count} struct requests; delta={untypedBytes - typedBytes}");
    }

    private static async Task<long> MeasureDrain(bool useTyped, int count)
    {
        long before = GC.GetTotalAllocatedBytes(precise: true);
        await Drain(useTyped, count);
        long after = GC.GetTotalAllocatedBytes(precise: true);
        return after - before;
    }

    private static async Task Drain(bool useTyped, int count)
    {
        using ActorSystem asx = new();

        // Classify everything as control so all messages are admitted (control is exempt from the bound)
        // and the classifier runs once per message.
        ActorRunnerOptions options = useTyped
            ? new ActorRunnerOptions<AllocProbeRequest> { IsControlMessage = static r => r.IsControl }
            : new ActorRunnerOptions { IsControlMessage = static o => ((AllocProbeRequest)o).IsControl };

        IActorRefStruct<AllocProbeStructActor, AllocProbeRequest, int> actor =
            asx.SpawnStructWithOptions<AllocProbeStructActor, AllocProbeRequest, int>(null, options);

        for (int i = 0; i < count; i++)
            actor.Send(new AllocProbeRequest { Id = i, IsControl = true });

        await asx.Wait();
    }
}
