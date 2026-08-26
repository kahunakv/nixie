
using Nixie.Tests.Actors;

namespace Nixie.Tests;

[Collection("Nixie")]
public sealed class TestTryAskPooled
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

    // ---- An admitted pooled ask completes with the handler's response ----

    [Fact]
    public async Task TestPooledAskRepliesWithResponse()
    {
        using ActorSystem asx = new();

        IActorRef<ThrowingReplyActor, ThrowingReplyRequest, ThrowingReplyResponse> actor =
            asx.Spawn<ThrowingReplyActor, ThrowingReplyRequest, ThrowingReplyResponse>();

        Assert.True(actor.TryAskPooled(new ThrowingReplyRequest { Value = "hello" }, out ValueTask<ThrowingReplyResponse?> reply));

        ThrowingReplyResponse? response = await reply;

        Assert.NotNull(response);
        Assert.Equal("hello", response.Value);
    }

    // ---- Sequential asks recycle the promise across generations without cross-talk ----

    [Fact]
    public async Task TestPooledAskSequentialAsksStayPairedAcrossRecycling()
    {
        using ActorSystem asx = new();

        IActorRef<ThrowingReplyActor, ThrowingReplyRequest, ThrowingReplyResponse> actor =
            asx.Spawn<ThrowingReplyActor, ThrowingReplyRequest, ThrowingReplyResponse>();

        for (int i = 0; i < 10_000; i++)
        {
            Assert.True(actor.TryAskPooled(new ThrowingReplyRequest { Value = i.ToString() }, out ValueTask<ThrowingReplyResponse?> reply));

            ThrowingReplyResponse? response = await reply;

            Assert.NotNull(response);
            Assert.Equal(i.ToString(), response.Value);
        }
    }

    // ---- Concurrent askers each get the reply to their own request (no cross-request contamination) ----

    [Fact]
    public async Task TestPooledAskConcurrentAsksStayPaired()
    {
        using ActorSystem asx = new();

        IActorRef<ThrowingReplyActor, ThrowingReplyRequest, ThrowingReplyResponse> actor =
            asx.Spawn<ThrowingReplyActor, ThrowingReplyRequest, ThrowingReplyResponse>();

        Task[] askers = new Task[8];

        for (int t = 0; t < askers.Length; t++)
        {
            int lane = t;

            askers[t] = Task.Run(async () =>
            {
                for (int i = 0; i < 500; i++)
                {
                    string expected = $"{lane}/{i}";

                    if (!actor.TryAskPooled(new ThrowingReplyRequest { Value = expected }, out ValueTask<ThrowingReplyResponse?> reply))
                        throw new Xunit.Sdk.XunitException("unbounded inbox rejected an ask");

                    ThrowingReplyResponse? response = await reply;

                    Assert.NotNull(response);
                    Assert.Equal(expected, response.Value);
                }
            });
        }

        await Task.WhenAll(askers);
    }

    // ---- A handler fault surfaces on the awaited reply ----

    [Fact]
    public async Task TestPooledAskHandlerFaultSurfacesException()
    {
        using ActorSystem asx = new();

        IActorRef<ThrowingReplyActor, ThrowingReplyRequest, ThrowingReplyResponse> actor =
            asx.Spawn<ThrowingReplyActor, ThrowingReplyRequest, ThrowingReplyResponse>();

        Assert.True(actor.TryAskPooled(new ThrowingReplyRequest { ShouldThrow = true }, out ValueTask<ThrowingReplyResponse?> reply));

        InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(async () => await reply);
        Assert.Equal("deliberate handler fault", ex.Message);

        // The faulted promise was recycled on consumption; the next ask answers normally.
        Assert.True(actor.TryAskPooled(new ThrowingReplyRequest { Value = "after" }, out ValueTask<ThrowingReplyResponse?> next));
        Assert.Equal("after", (await next)!.Value);
    }

    // ---- A shut-down runner rejects with a default ValueTask and enqueues nothing ----

    [Fact]
    public void TestPooledAskShutdownRejectsWithoutAllocation()
    {
        using ActorSystem asx = new();

        TaskCompletionSource gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        gate.SetResult();

        IActorRef<GatedReplyActor, TrySendRequest, TrySendResponse> actor =
            asx.SpawnWithOptions<GatedReplyActor, TrySendRequest, TrySendResponse>("tryaskpooled-gone", new ActorRunnerOptions
            {
                MaxInboxSize = 10
            }, gate);

        Assert.True(actor.Runner.Shutdown());

        Assert.False(actor.TryAskPooled(new TrySendRequest { Id = "x" }, out ValueTask<TrySendResponse?> reply));
        Assert.Equal(default, reply);
        Assert.Equal(0, actor.Runner.MessageCount);
    }

    // ---- At capacity, ordinary asks are rejected; control asks stay admitted and answered ----

    [Fact]
    public async Task TestPooledAskCapacityRejectsAndControlOvertakes()
    {
        using ActorSystem asx = new();

        TaskCompletionSource gate = new(TaskCreationOptions.RunContinuationsAsynchronously);

        IActorRef<GatedReplyActor, TrySendRequest, TrySendResponse> actor =
            asx.SpawnWithOptions<GatedReplyActor, TrySendRequest, TrySendResponse>("tryaskpooled-cap", new ActorRunnerOptions
            {
                MaxInboxSize = 2,
                IsControlMessage = m => ((TrySendRequest)m).IsControl
            }, gate);

        Assert.True(actor.TryAskPooled(new TrySendRequest { Id = "n0" }, out ValueTask<TrySendResponse?> r0));
        await WaitUntilInFlight(actor);

        Assert.True(actor.TryAskPooled(new TrySendRequest { Id = "n1" }, out ValueTask<TrySendResponse?> r1));
        Assert.True(actor.TryAskPooled(new TrySendRequest { Id = "n2" }, out ValueTask<TrySendResponse?> r2));

        Assert.False(actor.TryAskPooled(new TrySendRequest { Id = "n3" }, out ValueTask<TrySendResponse?> r3));
        Assert.Equal(default, r3);

        Assert.True(actor.TryAskPooled(new TrySendRequest { Id = "c0", IsControl = true }, out ValueTask<TrySendResponse?> rc));

        gate.SetResult();

        Assert.Equal("c0", (await rc)!.Id);
        Assert.Equal("n0", (await r0)!.Id);
        Assert.Equal("n1", (await r1)!.Id);
        Assert.Equal("n2", (await r2)!.Id);

        await asx.Wait();
        Assert.Equal(0, actor.Runner.MessageCount);
    }

    // ---- Shutdown after admission cancels the pending pooled replies ----

    [Fact]
    public async Task TestPooledAskShutdownCancelsAdmittedPendingReplies()
    {
        using ActorSystem asx = new();

        TaskCompletionSource gate = new(TaskCreationOptions.RunContinuationsAsynchronously);

        IActorRef<GatedReplyActor, TrySendRequest, TrySendResponse> actor =
            asx.SpawnWithOptions<GatedReplyActor, TrySendRequest, TrySendResponse>("tryaskpooled-drain", new ActorRunnerOptions
            {
                MaxInboxSize = 10
            }, gate);

        Assert.True(actor.TryAskPooled(new TrySendRequest { Id = "n0" }, out ValueTask<TrySendResponse?> r0));
        await WaitUntilInFlight(actor);

        Assert.True(actor.TryAskPooled(new TrySendRequest { Id = "n1" }, out ValueTask<TrySendResponse?> r1));
        Assert.True(actor.TryAskPooled(new TrySendRequest { Id = "n2" }, out ValueTask<TrySendResponse?> r2));

        // Shutdown sweeps the queued (not yet delivered) messages, cancelling their pooled promises.
        Assert.True(actor.Runner.Shutdown());
        gate.SetResult();

        await Assert.ThrowsAsync<TaskCanceledException>(async () => await r1);
        await Assert.ThrowsAsync<TaskCanceledException>(async () => await r2);
    }

    // ---- A deferred handler completes the pooled reply later via its handle; the handle is
    // ---- first-wins and goes stale after consumption ----

    [Fact]
    public async Task TestPooledAskDeferredCompletionViaHandle()
    {
        using ActorSystem asx = new();

        IActorRef<DeferredPooledReplyActor, TrySendRequest, TrySendResponse> actor =
            asx.Spawn<DeferredPooledReplyActor, TrySendRequest, TrySendResponse>();

        Assert.True(actor.TryAskPooled(new TrySendRequest { Id = "deferred" }, out ValueTask<TrySendResponse?> reply));

        TrySendResponse? response = await reply;

        Assert.NotNull(response);
        Assert.Equal("deferred", response.Id);

        // The captured handle already completed its generation and the promise was consumed:
        // a late completion attempt must be rejected instead of touching a recycled promise.
        Assert.True(((DeferredPooledReplyActor)actor.Runner.Actor!).Handles.TryDequeue(out ReplyHandle<TrySendResponse> handle));
        Assert.False(handle.TrySetResult(new TrySendResponse { Id = "stale" }));

        // A fresh ask after the stale attempt still answers correctly.
        Assert.True(actor.TryAskPooled(new TrySendRequest { Id = "next" }, out ValueTask<TrySendResponse?> next));
        Assert.Equal("next", (await next)!.Id);
    }

    // ---- The default handle is inert ----

    [Fact]
    public void TestDefaultHandleIsInert()
    {
        ReplyHandle<TrySendResponse> handle = default;

        Assert.True(handle.IsDefault);
        Assert.True(handle.IsCompleted);
        Assert.False(handle.TrySetResult(new TrySendResponse { Id = "x" }));
        Assert.False(handle.TrySetException(new InvalidOperationException()));
        Assert.False(handle.TrySetCanceled());
    }
}
