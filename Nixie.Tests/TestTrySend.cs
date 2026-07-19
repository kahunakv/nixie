
using Nixie.Tests.Actors;

namespace Nixie.Tests;

[Collection("Nixie")]
public sealed class TestTrySend
{
    // Polls until the actor has picked up its first message and is blocked inside the handler (drainer
    // running, inbox drained), so the bounded inbox can then be filled deterministically.
    private static async Task WaitUntilInFlight<TActor, TRequest, TResponse>(
        IActorRef<TActor, TRequest, TResponse> actor)
        where TActor : IActor<TRequest, TResponse> where TRequest : class where TResponse : class?
    {
        for (int i = 0; i < 200; i++)
        {
            if (actor.Runner.IsProcessing && actor.Runner.MessageCount == 0)
            {
                await Task.Delay(30);   // let Receive actually enter its gate await
                return;
            }

            await Task.Delay(10);
        }

        throw new Xunit.Sdk.XunitException("actor never reached the in-flight state");
    }

    // ---- Reply runner: at capacity, ordinary is rejected (handler never runs), control still admitted ----

    [Fact]
    public async Task TestReplyAtCapacityRejectsOrdinaryButAdmitsControl()
    {
        using ActorSystem asx = new();

        TaskCompletionSource gate = new(TaskCreationOptions.RunContinuationsAsynchronously);

        IActorRef<GatedReplyActor, TrySendRequest, TrySendResponse> actor =
            asx.SpawnWithOptions<GatedReplyActor, TrySendRequest, TrySendResponse>("cap", new ActorRunnerOptions
            {
                MaxInboxSize = 2,
                IsControlMessage = m => ((TrySendRequest)m).IsControl
            }, gate);

        // n0 is admitted and blocks the drainer on the gate.
        Assert.True(actor.TrySend(new TrySendRequest { Id = "n0" }));
        await WaitUntilInFlight(actor);

        // Fill the ordinary inbox to capacity (depth 2).
        Assert.True(actor.TrySend(new TrySendRequest { Id = "n1" }));
        Assert.True(actor.TrySend(new TrySendRequest { Id = "n2" }));

        // A third ordinary message is rejected — never delivered, safe to retry.
        Assert.False(actor.TrySend(new TrySendRequest { Id = "n3" }));

        // A control message is admitted even though the ordinary inbox is full (exempt from the bound).
        Assert.True(actor.TrySend(new TrySendRequest { Id = "c0", IsControl = true }));

        gate.SetResult();
        await asx.Wait();

        GatedReplyActor impl = (GatedReplyActor)actor.Runner.Actor!;
        string[] order = [.. impl.Processed];

        Assert.Equal("n0", order[0]);
        int ic = Array.IndexOf(order, "c0");
        int i1 = Array.IndexOf(order, "n1");
        int i2 = Array.IndexOf(order, "n2");
        Assert.True(ic >= 0 && ic < i1 && ic < i2, "control must overtake the queued ordinary messages");
        Assert.True(i1 < i2, "FIFO within the ordinary class");
        Assert.DoesNotContain("n3", order);              // rejected, handler never ran
        Assert.Equal(4, order.Length);
        Assert.Equal(0, actor.Runner.MessageCount);
    }

    // ---- Reply runner: a shut-down runner rejects and enqueues nothing ----

    [Fact]
    public void TestReplyShutdownRejectsAndEnqueuesNothing()
    {
        using ActorSystem asx = new();

        TaskCompletionSource gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        gate.SetResult();

        IActorRef<GatedReplyActor, TrySendRequest, TrySendResponse> actor =
            asx.SpawnWithOptions<GatedReplyActor, TrySendRequest, TrySendResponse>("gone", new ActorRunnerOptions
            {
                MaxInboxSize = 10
            }, gate);

        Assert.True(actor.Runner.Shutdown());

        Assert.False(actor.TrySend(new TrySendRequest { Id = "x" }));
        Assert.False(actor.TrySend(new TrySendRequest { Id = "y" }, (IGenericActorRef)actor));
        Assert.Equal(0, actor.Runner.MessageCount);
    }

    // ---- Reply runner: an unbounded, live runner always admits and behaves like Send ----

    [Fact]
    public async Task TestReplyUnboundedAlwaysAdmits()
    {
        using ActorSystem asx = new();

        TaskCompletionSource gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        gate.SetResult();   // never block

        IActorRef<GatedReplyActor, TrySendRequest, TrySendResponse> actor =
            asx.Spawn<GatedReplyActor, TrySendRequest, TrySendResponse>("unbounded", gate);

        const int total = 500;
        for (int i = 0; i < total; i++)
            Assert.True(actor.TrySend(new TrySendRequest { Id = $"m{i}" }));

        await asx.Wait();

        GatedReplyActor impl = (GatedReplyActor)actor.Runner.Actor!;
        Assert.Equal(total, impl.Processed.Count);
        Assert.Equal(0, actor.Runner.MessageCount);
    }

    // ---- Reply runner: a burst of rejected TrySends leaves no unobserved task (clean GC) ----

    [Fact]
    public async Task TestRejectedTrySendsLeaveNoUnobservedTask()
    {
        int unobserved = 0;
        EventHandler<UnobservedTaskExceptionEventArgs> handler = (_, e) =>
        {
            Interlocked.Increment(ref unobserved);
            e.SetObserved();
        };

        TaskScheduler.UnobservedTaskException += handler;
        try
        {
            using ActorSystem asx = new();

            TaskCompletionSource gate = new(TaskCreationOptions.RunContinuationsAsynchronously);

            IActorRef<GatedReplyActor, TrySendRequest, TrySendResponse> actor =
                asx.SpawnWithOptions<GatedReplyActor, TrySendRequest, TrySendResponse>("burst", new ActorRunnerOptions
                {
                    MaxInboxSize = 2
                }, gate);

            Assert.True(actor.TrySend(new TrySendRequest { Id = "n0" }));
            await WaitUntilInFlight(actor);

            Assert.True(actor.TrySend(new TrySendRequest { Id = "n1" }));
            Assert.True(actor.TrySend(new TrySendRequest { Id = "n2" }));

            // Hundreds of rejections: the clean path allocates no promise, so none can become unobserved.
            int rejected = 0;
            for (int i = 0; i < 500; i++)
            {
                if (!actor.TrySend(new TrySendRequest { Id = $"r{i}" }))
                    rejected++;
            }

            Assert.Equal(500, rejected);

            gate.SetResult();
            await asx.Wait();

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            Assert.Equal(0, Volatile.Read(ref unobserved));
        }
        finally
        {
            TaskScheduler.UnobservedTaskException -= handler;
        }
    }

    // ---- Struct-reply runner: at capacity, ordinary rejected, control admitted ----

    [Fact]
    public async Task TestStructAtCapacityRejectsOrdinaryButAdmitsControl()
    {
        using ActorSystem asx = new();

        TaskCompletionSource gate = new(TaskCreationOptions.RunContinuationsAsynchronously);

        IActorRefStruct<GatedStructActor, TrySendStructRequest, int> actor =
            asx.SpawnStructWithOptions<GatedStructActor, TrySendStructRequest, int>("cap-struct", new ActorRunnerOptions
            {
                MaxInboxSize = 2,
                IsControlMessage = m => ((TrySendStructRequest)m).IsControl
            }, gate);

        Assert.True(actor.TrySend(new TrySendStructRequest { Id = 0 }));

        for (int i = 0; i < 200; i++)
        {
            if (actor.Runner.IsProcessing && actor.Runner.MessageCount == 0)
            {
                await Task.Delay(30);
                break;
            }

            await Task.Delay(10);
        }

        Assert.True(actor.TrySend(new TrySendStructRequest { Id = 1 }));
        Assert.True(actor.TrySend(new TrySendStructRequest { Id = 2 }));
        Assert.False(actor.TrySend(new TrySendStructRequest { Id = 3 }));
        Assert.True(actor.TrySend(new TrySendStructRequest { Id = 99, IsControl = true }));

        gate.SetResult();
        await asx.Wait();

        GatedStructActor impl = (GatedStructActor)actor.Runner.Actor!;
        int[] order = [.. impl.Processed];

        Assert.Equal(0, order[0]);
        int ic = Array.IndexOf(order, 99);
        int i1 = Array.IndexOf(order, 1);
        int i2 = Array.IndexOf(order, 2);
        Assert.True(ic >= 0 && ic < i1 && ic < i2, "control must overtake queued ordinary messages");
        Assert.True(i1 < i2, "FIFO within the ordinary class");
        Assert.DoesNotContain(3, order);
    }

    // ---- Aggregate-reply runner: at capacity, ordinary rejected, control admitted (batched first) ----

    [Fact]
    public async Task TestAggregateAtCapacityRejectsOrdinaryButAdmitsControl()
    {
        using ActorSystem asx = new();

        TaskCompletionSource gate = new(TaskCreationOptions.RunContinuationsAsynchronously);

        IActorRefAggregate<GatedAggregateActor, TrySendRequest, TrySendResponse> actor =
            asx.SpawnAggregateWithOptions<GatedAggregateActor, TrySendRequest, TrySendResponse>("cap-agg", new ActorRunnerOptions
            {
                MaxInboxSize = 2,
                IsControlMessage = m => ((TrySendRequest)m).IsControl
            }, gate);

        Assert.True(actor.TrySend(new TrySendRequest { Id = "n0" }));

        for (int i = 0; i < 200; i++)
        {
            if (actor.Runner.IsProcessing && actor.Runner.MessageCount == 0)
            {
                await Task.Delay(30);
                break;
            }

            await Task.Delay(10);
        }

        Assert.True(actor.TrySend(new TrySendRequest { Id = "n1" }));
        Assert.True(actor.TrySend(new TrySendRequest { Id = "n2" }));
        Assert.False(actor.TrySend(new TrySendRequest { Id = "n3" }));
        Assert.True(actor.TrySend(new TrySendRequest { Id = "c0", IsControl = true }));

        gate.SetResult();
        await asx.Wait();

        GatedAggregateActor impl = (GatedAggregateActor)actor.Runner.Actor!;
        string[] order = [.. impl.Processed];

        Assert.Equal("n0", order[0]);
        int ic = Array.IndexOf(order, "c0");
        int i1 = Array.IndexOf(order, "n1");
        int i2 = Array.IndexOf(order, "n2");
        Assert.True(ic >= 0 && ic < i1 && ic < i2, "control must be batched ahead of queued ordinary messages");
        Assert.True(i1 < i2, "FIFO within the ordinary class");
        Assert.DoesNotContain("n3", order);
    }
}
