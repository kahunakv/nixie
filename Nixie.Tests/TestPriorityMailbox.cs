
using Nixie.Tests.Actors;

namespace Nixie.Tests;

[Collection("Nixie")]
public sealed class TestPriorityMailbox
{
    // ---- Reply runner: control is exempt from the bound, overtakes normal, FIFO within class ----

    [Fact]
    public async Task TestControlExemptFromBoundOvertakesNormalAndPreservesBackpressure()
    {
        using ActorSystem asx = new();

        IActorRef<PriorityBlockingActor, PriorityRequest, PriorityResponse> actor =
            asx.SpawnWithOptions<PriorityBlockingActor, PriorityRequest, PriorityResponse>("hot", new ActorRunnerOptions
            {
                MaxInboxSize = 2,
                IsControlMessage = m => ((PriorityRequest)m).IsControl
            });

        // n0 starts processing and blocks the drainer (~1.5 s).
        Task<PriorityResponse?> n0 = actor.Ask(new PriorityRequest { Id = "n0" });
        await Task.Delay(100);

        // Fill the normal queue to capacity (depth 2).
        Task<PriorityResponse?> n1 = actor.Ask(new PriorityRequest { Id = "n1" });
        Task<PriorityResponse?> n2 = actor.Ask(new PriorityRequest { Id = "n2" });

        // A third normal is rejected — backpressure preserved.
        await Assert.ThrowsAsync<ActorBusyException>(async () =>
            await actor.Ask(new PriorityRequest { Id = "n3" }));

        // A control message is admitted even though the normal inbox is full (exempt from the bound).
        actor.Send(new PriorityRequest { Id = "c0", IsControl = true });

        await Task.WhenAll(n0, n1, n2);
        await asx.Wait();

        PriorityBlockingActor impl = (PriorityBlockingActor)actor.Runner.Actor!;
        string[] order = [.. impl.ProcessedOrder];

        Assert.Equal("n0", order[0]);                          // already in flight, runs first
        int ic = Array.IndexOf(order, "c0");
        int i1 = Array.IndexOf(order, "n1");
        int i2 = Array.IndexOf(order, "n2");
        Assert.True(ic >= 0, "control message was never delivered");
        Assert.True(ic < i1 && ic < i2, "control must overtake the queued normal messages");
        Assert.True(i1 < i2, "FIFO must be preserved within the normal class");
        Assert.DoesNotContain("n3", order);                    // rejected, never processed
        Assert.Equal(4, order.Length);
        Assert.Equal(0, actor.Runner.MessageCount);
    }

    // ---- Without a predicate, a "control-shaped" message is just an ordinary message ----

    [Fact]
    public async Task TestWithoutPredicateControlShapedMessageIsOrdinaryAndBounded()
    {
        using ActorSystem asx = new();

        // No IsControlMessage predicate: IsControl=true carries no special meaning.
        IActorRef<PriorityBlockingActor, PriorityRequest, PriorityResponse> actor =
            asx.SpawnWithOptions<PriorityBlockingActor, PriorityRequest, PriorityResponse>("plain", new ActorRunnerOptions
            {
                MaxInboxSize = 1
            });

        Task<PriorityResponse?> n0 = actor.Ask(new PriorityRequest { Id = "n0" });
        await Task.Delay(100);

        // Occupies the single slot as an ordinary message.
        Task<PriorityResponse?> shaped = actor.Ask(new PriorityRequest { Id = "shaped", IsControl = true });

        // With the slot taken, the next message is rejected — proving the control-shaped one was NOT exempt.
        await Assert.ThrowsAsync<ActorBusyException>(async () =>
            await actor.Ask(new PriorityRequest { Id = "overflow" }));

        await Task.WhenAll(n0, shaped);
        await asx.Wait();
    }

    // ---- Struct-reply runner: same guarantee ----

    [Fact]
    public async Task TestStructReplyControlExemptOvertakesNormal()
    {
        using ActorSystem asx = new();

        IActorRefStruct<PriorityStructActor, PriorityStructRequest, int> actor =
            asx.SpawnStructWithOptions<PriorityStructActor, PriorityStructRequest, int>("hot-struct", new ActorRunnerOptions
            {
                MaxInboxSize = 2,
                IsControlMessage = m => ((PriorityStructRequest)m).IsControl
            });

        Task<int> n0 = actor.Ask(new PriorityStructRequest { Id = 0 });
        await Task.Delay(100);

        Task<int> n1 = actor.Ask(new PriorityStructRequest { Id = 1 });
        Task<int> n2 = actor.Ask(new PriorityStructRequest { Id = 2 });

        await Assert.ThrowsAsync<ActorBusyException>(async () =>
            await actor.Ask(new PriorityStructRequest { Id = 3 }));

        actor.Send(new PriorityStructRequest { Id = 99, IsControl = true });

        await Task.WhenAll(n0, n1, n2);
        await asx.Wait();

        PriorityStructActor impl = (PriorityStructActor)actor.Runner.Actor!;
        int[] order = [.. impl.ProcessedOrder];

        Assert.Equal(0, order[0]);
        int ic = Array.IndexOf(order, 99);
        int i1 = Array.IndexOf(order, 1);
        int i2 = Array.IndexOf(order, 2);
        Assert.True(ic >= 0 && ic < i1 && ic < i2, "control must overtake queued normals");
        Assert.True(i1 < i2, "FIFO within the normal class");
        Assert.DoesNotContain(3, order);
    }

    // ---- Aggregate-reply runner: control lands first in the batch ----

    [Fact]
    public async Task TestAggregateReplyControlExemptOvertakesNormal()
    {
        using ActorSystem asx = new();

        IActorRefAggregate<PriorityAggregateActor, PriorityRequest, PriorityResponse> actor =
            asx.SpawnAggregateWithOptions<PriorityAggregateActor, PriorityRequest, PriorityResponse>("hot-agg", new ActorRunnerOptions
            {
                MaxInboxSize = 2,
                IsControlMessage = m => ((PriorityRequest)m).IsControl
            });

        // First batch = [n0]; it blocks the drainer (~0.5 s) while the rest arrive.
        Task<PriorityResponse?> n0 = actor.Ask(new PriorityRequest { Id = "n0" });
        await Task.Delay(100);

        Task<PriorityResponse?> n1 = actor.Ask(new PriorityRequest { Id = "n1" });
        Task<PriorityResponse?> n2 = actor.Ask(new PriorityRequest { Id = "n2" });

        await Assert.ThrowsAsync<ActorBusyException>(async () =>
            await actor.Ask(new PriorityRequest { Id = "n3" }));

        actor.Send(new PriorityRequest { Id = "c0", IsControl = true });

        await Task.WhenAll(n0, n1, n2);
        await asx.Wait();

        PriorityAggregateActor impl = (PriorityAggregateActor)actor.Runner.Actor!;
        string[] order = [.. impl.ProcessedOrder];

        int ic = Array.IndexOf(order, "c0");
        int i1 = Array.IndexOf(order, "n1");
        int i2 = Array.IndexOf(order, "n2");
        Assert.True(ic >= 0 && ic < i1 && ic < i2, "control must be batched ahead of queued normals");
        Assert.True(i1 < i2, "FIFO within the normal class");
        Assert.DoesNotContain("n3", order);
    }

    // ---- Stress: flood normal reads while control completions must all get through ----

    [Fact]
    public async Task TestStressControlCompletionsAreNeverStranded()
    {
        using ActorSystem asx = new();

        IActorRef<SelfCompletingReadActor, ReadRequest, ReadResponse> actor =
            asx.SpawnWithOptions<SelfCompletingReadActor, ReadRequest, ReadResponse>("kv", new ActorRunnerOptions
            {
                MaxInboxSize = 5,
                IsControlMessage = m => ((ReadRequest)m).IsControl
            });

        const int total = 100;
        List<Task<ReadResponse?>> reads = [];

        for (int i = 0; i < total; i++)
            reads.Add(SafeRead(actor, $"r{i}"));

        Task<ReadResponse?[]> all = Task.WhenAll(reads);
        Task finished = await Task.WhenAny(all, Task.Delay(TimeSpan.FromSeconds(30)));

        Assert.True(finished == all, "a read never resolved — a control completion was stranded by the bound");

        await asx.Wait();

        SelfCompletingReadActor impl = (SelfCompletingReadActor)actor.Runner.Actor!;

        // Every admitted read received its control completion (none stranded).
        Assert.True(impl.Admitted > 0);
        Assert.Equal(impl.Admitted, impl.Resumed);
        Assert.Equal(0, actor.Runner.MessageCount);

        // Every read task either completed (admitted → resolved) or was rejected pre-admission.
        ReadResponse?[] results = await all;
        int completed = results.Count(r => r is not null);
        Assert.Equal(impl.Admitted, completed);
    }

    private static async Task<ReadResponse?> SafeRead(
        IActorRef<SelfCompletingReadActor, ReadRequest, ReadResponse> actor, string id)
    {
        try
        {
            return await actor.Ask(new ReadRequest { Kind = "read", Id = id });
        }
        catch (ActorBusyException)
        {
            return null;   // rejected pre-admission — safe to retry, not stranded
        }
    }
}
