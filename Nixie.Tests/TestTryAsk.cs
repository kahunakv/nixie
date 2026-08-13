
using Nixie.Tests.Actors;

namespace Nixie.Tests;

[Collection("Nixie")]
public sealed class TestTryAsk
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

    // ---- Reply runner: admitted asks complete with their responses; at capacity ordinary is rejected
    // with no reply task, while control is still admitted and replied to ----

    [Fact]
    public async Task TestReplyAdmittedAsksCompleteAndCapacityRejectsWithoutAllocation()
    {
        using ActorSystem asx = new();

        TaskCompletionSource gate = new(TaskCreationOptions.RunContinuationsAsynchronously);

        IActorRef<GatedReplyActor, TrySendRequest, TrySendResponse> actor =
            asx.SpawnWithOptions<GatedReplyActor, TrySendRequest, TrySendResponse>("tryask-cap", new ActorRunnerOptions
            {
                MaxInboxSize = 2,
                IsControlMessage = m => ((TrySendRequest)m).IsControl
            }, gate);

        // n0 is admitted and blocks the drainer on the gate.
        Assert.True(actor.TryAsk(new TrySendRequest { Id = "n0" }, out Task<TrySendResponse?>? r0));
        Assert.NotNull(r0);
        await WaitUntilInFlight(actor);

        // Fill the ordinary inbox to capacity (depth 2).
        Assert.True(actor.TryAsk(new TrySendRequest { Id = "n1" }, out Task<TrySendResponse?>? r1));
        Assert.True(actor.TryAsk(new TrySendRequest { Id = "n2" }, out Task<TrySendResponse?>? r2));

        // A third ordinary message is rejected: false, no reply task, nothing enqueued.
        Assert.False(actor.TryAsk(new TrySendRequest { Id = "n3" }, out Task<TrySendResponse?>? r3));
        Assert.Null(r3);

        // A control message is admitted even though the ordinary inbox is full (exempt from the bound).
        Assert.True(actor.TryAsk(new TrySendRequest { Id = "c0", IsControl = true }, out Task<TrySendResponse?>? rc));

        gate.SetResult();

        Assert.Equal("c0", (await rc!)!.Id);
        Assert.Equal("n0", (await r0!)!.Id);
        Assert.Equal("n1", (await r1!)!.Id);
        Assert.Equal("n2", (await r2!)!.Id);

        await asx.Wait();
        Assert.Equal(0, actor.Runner.MessageCount);
    }

    // ---- Reply runner: a shut-down runner rejects with no reply task and enqueues nothing ----

    [Fact]
    public void TestReplyShutdownRejectsWithoutAllocation()
    {
        using ActorSystem asx = new();

        TaskCompletionSource gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        gate.SetResult();

        IActorRef<GatedReplyActor, TrySendRequest, TrySendResponse> actor =
            asx.SpawnWithOptions<GatedReplyActor, TrySendRequest, TrySendResponse>("tryask-gone", new ActorRunnerOptions
            {
                MaxInboxSize = 10
            }, gate);

        Assert.True(actor.Runner.Shutdown());

        Assert.False(actor.TryAsk(new TrySendRequest { Id = "x" }, out Task<TrySendResponse?>? rx));
        Assert.Null(rx);
        Assert.False(actor.TryAsk(new TrySendRequest { Id = "y" }, (IGenericActorRef)actor, out Task<TrySendResponse?>? ry));
        Assert.Null(ry);
        Assert.Equal(0, actor.Runner.MessageCount);
    }

    // ---- Reply runner: shutdown after admission cancels the pending replies (no promise left hanging) ----

    [Fact]
    public async Task TestReplyShutdownCancelsAdmittedPendingReplies()
    {
        using ActorSystem asx = new();

        TaskCompletionSource gate = new(TaskCreationOptions.RunContinuationsAsynchronously);

        IActorRef<GatedReplyActor, TrySendRequest, TrySendResponse> actor =
            asx.SpawnWithOptions<GatedReplyActor, TrySendRequest, TrySendResponse>("tryask-drain", new ActorRunnerOptions
            {
                MaxInboxSize = 10
            }, gate);

        Assert.True(actor.TryAsk(new TrySendRequest { Id = "n0" }, out Task<TrySendResponse?>? r0));
        await WaitUntilInFlight(actor);

        Assert.True(actor.TryAsk(new TrySendRequest { Id = "n1" }, out Task<TrySendResponse?>? r1));
        Assert.True(actor.TryAsk(new TrySendRequest { Id = "n2" }, out Task<TrySendResponse?>? r2));

        // Shutdown sweeps the queued (not yet delivered) messages, cancelling their promises.
        Assert.True(actor.Runner.Shutdown());

        await Assert.ThrowsAsync<TaskCanceledException>(async () => await r1!);
        await Assert.ThrowsAsync<TaskCanceledException>(async () => await r2!);

        // The in-flight message was already dequeued; it finishes normally once the gate opens.
        gate.SetResult();
        Assert.Equal("n0", (await r0!)!.Id);
    }

    // ---- Reply runner: a burst of rejected TryAsks leaves no unobserved task (clean GC) ----

    [Fact]
    public async Task TestRejectedTryAsksLeaveNoUnobservedTask()
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
                asx.SpawnWithOptions<GatedReplyActor, TrySendRequest, TrySendResponse>("tryask-burst", new ActorRunnerOptions
                {
                    MaxInboxSize = 2
                }, gate);

            Assert.True(actor.TryAsk(new TrySendRequest { Id = "n0" }, out Task<TrySendResponse?>? r0));
            await WaitUntilInFlight(actor);

            Assert.True(actor.TryAsk(new TrySendRequest { Id = "n1" }, out Task<TrySendResponse?>? r1));
            Assert.True(actor.TryAsk(new TrySendRequest { Id = "n2" }, out Task<TrySendResponse?>? r2));

            // Hundreds of rejections: the clean path allocates no promise and no exception, so none can
            // become an unobserved faulted task (unlike Ask, which faults a promise with ActorBusyException).
            int rejected = 0;
            for (int i = 0; i < 500; i++)
            {
                if (!actor.TryAsk(new TrySendRequest { Id = $"r{i}" }, out _))
                    rejected++;
            }

            Assert.Equal(500, rejected);

            gate.SetResult();
            await Task.WhenAll(r0!, r1!, r2!);
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

    // ---- Struct-reply runner: admitted asks complete with their responses; capacity rejects cleanly ----

    [Fact]
    public async Task TestStructAdmittedAsksCompleteAndCapacityRejects()
    {
        using ActorSystem asx = new();

        TaskCompletionSource gate = new(TaskCreationOptions.RunContinuationsAsynchronously);

        IActorRefStruct<GatedStructActor, TrySendStructRequest, int> actor =
            asx.SpawnStructWithOptions<GatedStructActor, TrySendStructRequest, int>("tryask-struct", new ActorRunnerOptions
            {
                MaxInboxSize = 2,
                IsControlMessage = m => ((TrySendStructRequest)m).IsControl
            }, gate);

        Assert.True(actor.TryAsk(new TrySendStructRequest { Id = 0 }, out Task<int>? r0));

        for (int i = 0; i < 200; i++)
        {
            if (actor.Runner.IsProcessing && actor.Runner.MessageCount == 0)
            {
                await Task.Delay(30);
                break;
            }

            await Task.Delay(10);
        }

        Assert.True(actor.TryAsk(new TrySendStructRequest { Id = 1 }, out Task<int>? r1));
        Assert.True(actor.TryAsk(new TrySendStructRequest { Id = 2 }, out Task<int>? r2));
        Assert.False(actor.TryAsk(new TrySendStructRequest { Id = 3 }, out Task<int>? r3));
        Assert.Null(r3);
        Assert.True(actor.TryAsk(new TrySendStructRequest { Id = 99, IsControl = true }, out Task<int>? rc));

        gate.SetResult();

        Assert.Equal(99, await rc!);
        Assert.Equal(0, await r0!);
        Assert.Equal(1, await r1!);
        Assert.Equal(2, await r2!);

        await asx.Wait();
    }

    // ---- Aggregate-reply runner: admitted asks complete with their responses; capacity rejects cleanly ----

    [Fact]
    public async Task TestAggregateAdmittedAsksCompleteAndCapacityRejects()
    {
        using ActorSystem asx = new();

        TaskCompletionSource gate = new(TaskCreationOptions.RunContinuationsAsynchronously);

        IActorRefAggregate<GatedAggregateActor, TrySendRequest, TrySendResponse> actor =
            asx.SpawnAggregateWithOptions<GatedAggregateActor, TrySendRequest, TrySendResponse>("tryask-agg", new ActorRunnerOptions
            {
                MaxInboxSize = 2,
                IsControlMessage = m => ((TrySendRequest)m).IsControl
            }, gate);

        Assert.True(actor.TryAsk(new TrySendRequest { Id = "n0" }, out Task<TrySendResponse?>? r0));

        for (int i = 0; i < 200; i++)
        {
            if (actor.Runner.IsProcessing && actor.Runner.MessageCount == 0)
            {
                await Task.Delay(30);
                break;
            }

            await Task.Delay(10);
        }

        Assert.True(actor.TryAsk(new TrySendRequest { Id = "n1" }, out Task<TrySendResponse?>? r1));
        Assert.True(actor.TryAsk(new TrySendRequest { Id = "n2" }, out Task<TrySendResponse?>? r2));
        Assert.False(actor.TryAsk(new TrySendRequest { Id = "n3" }, out Task<TrySendResponse?>? r3));
        Assert.Null(r3);
        Assert.True(actor.TryAsk(new TrySendRequest { Id = "c0", IsControl = true }, out Task<TrySendResponse?>? rc));

        gate.SetResult();

        Assert.Equal("n0", (await r0!)!.Id);
        Assert.Equal("n1", (await r1!)!.Id);
        Assert.Equal("n2", (await r2!)!.Id);
        Assert.Equal("c0", (await rc!)!.Id);

        await asx.Wait();
        Assert.Equal(0, actor.Runner.MessageCount);
    }
}
