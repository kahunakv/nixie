
using Nixie.Tests.Actors;

namespace Nixie.Tests;

[Collection("Nixie")]
public sealed class TestAskCancellation
{
    // ---- Change 1: CancellationToken overloads ----

    [Fact]
    public async Task TestAskWithAlreadyCancelledTokenThrowsBeforeAdmission()
    {
        using ActorSystem asx = new();

        IActorRef<ReplyActor, string, string> actor = asx.Spawn<ReplyActor, string, string>();

        using CancellationTokenSource cts = new();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await actor.Ask("never", cts.Token));

        // Nothing was admitted.
        await asx.Wait();
        Assert.Equal(0, actor.Runner.MessageCount);
    }

    [Fact]
    public async Task TestAskCancelledWhileQueuedSurfacesOperationCanceled()
    {
        using ActorSystem asx = new();

        IActorRef<ReplySlowActor, string, string> actor = asx.Spawn<ReplySlowActor, string, string>();

        // Occupy the drainer with a slow (2 s) message.
        Task<string?> hold = actor.Ask("hold");
        await Task.Delay(50);

        using CancellationTokenSource cts = new();
        Task<string?> victim = actor.Ask("victim", cts.Token);   // queued behind "hold"

        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await victim);

        Assert.Equal("hold", await hold);
    }

    // ---- Change 2: message cancelled before processing is never delivered ----

    [Fact]
    public async Task TestCancelledQueuedMessageIsNeverDelivered()
    {
        using ActorSystem asx = new();

        IActorRef<ReplySlowActor, string, string> actor = asx.Spawn<ReplySlowActor, string, string>();

        Task<string?> hold = actor.Ask("hold");
        await Task.Delay(50);   // drainer is busy processing "hold"

        using CancellationTokenSource cts = new();
        Task<string?> victim = actor.Ask("victim", cts.Token);   // queued, not yet dequeued
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await victim);

        // Let the drainer finish "hold" and reach (and skip) "victim".
        Assert.Equal("hold", await hold);
        await asx.Wait();

        ReplySlowActor impl = (ReplySlowActor)actor.Runner.Actor!;
        Assert.Equal(0, impl.GetMessages("victim"));
        Assert.Equal(1, impl.GetMessages("hold"));
        Assert.Equal(0, actor.Runner.MessageCount);
    }

    [Fact]
    public async Task TestInFlightMessageStillCompletesWhenCancelled()
    {
        using ActorSystem asx = new();

        IActorRef<ReplySlowActor, string, string> actor = asx.Spawn<ReplySlowActor, string, string>();

        using CancellationTokenSource cts = new();
        Task<string?> inflight = actor.Ask("inflight", cts.Token);

        await Task.Delay(50);   // message already dequeued and inside Receive
        await cts.CancelAsync();

        // Caller observes cancellation...
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await inflight);

        // ...but the already-started handler runs to completion; no exception escapes the runner.
        await asx.Wait();
        ReplySlowActor impl = (ReplySlowActor)actor.Runner.Actor!;
        Assert.Equal(1, impl.GetMessages("inflight"));
    }

    [Fact]
    public async Task TestUncancelledTokenReturnsResultNormally()
    {
        using ActorSystem asx = new();

        IActorRef<ReplyActor, string, string> actor = asx.Spawn<ReplyActor, string, string>();

        using CancellationTokenSource cts = new();
        string? reply = await actor.Ask("hello", cts.Token);

        Assert.Equal("hello", reply);
    }

    // ---- Change 3: timeout resolves the promise and skips the queued message ----

    [Fact]
    public async Task TestTimeoutSkipsQueuedMessageAndThrowsAskTimeout()
    {
        using ActorSystem asx = new();

        IActorRef<ReplySlowActor, string, string> actor = asx.Spawn<ReplySlowActor, string, string>();

        Task<string?> hold = actor.Ask("hold");
        await Task.Delay(50);   // drainer busy for ~2 s

        // Queued behind "hold"; its 100 ms timeout fires long before the drainer reaches it.
        await Assert.ThrowsAsync<AskTimeoutException>(async () =>
            await actor.Ask("timed-out", TimeSpan.FromMilliseconds(100)));

        Assert.Equal("hold", await hold);
        await asx.Wait();

        ReplySlowActor impl = (ReplySlowActor)actor.Runner.Actor!;
        Assert.Equal(0, impl.GetMessages("timed-out"));   // never delivered
        Assert.Equal(0, actor.Runner.MessageCount);       // no orphaned pending count
    }

    [Fact]
    public async Task TestTimeoutWithTokenOverloadThrowsAskTimeoutOnTimeout()
    {
        using ActorSystem asx = new();

        IActorRef<ReplySlowActor, string, string> actor = asx.Spawn<ReplySlowActor, string, string>();

        Task<string?> hold = actor.Ask("hold");
        await Task.Delay(50);

        using CancellationTokenSource cts = new();   // never cancelled

        await Assert.ThrowsAsync<AskTimeoutException>(async () =>
            await actor.Ask("timed-out", TimeSpan.FromMilliseconds(100), cts.Token));

        Assert.Equal("hold", await hold);
    }

    [Fact]
    public async Task TestTimeoutWithTokenOverloadThrowsOperationCanceledOnTokenTrip()
    {
        using ActorSystem asx = new();

        IActorRef<ReplySlowActor, string, string> actor = asx.Spawn<ReplySlowActor, string, string>();

        Task<string?> hold = actor.Ask("hold");
        await Task.Delay(50);

        using CancellationTokenSource cts = new();
        // Generous timeout; the token trips first.
        Task<string?> victim = actor.Ask("victim", TimeSpan.FromSeconds(30), cts.Token);
        await cts.CancelAsync();

        OperationCanceledException ex =
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await victim);
        Assert.IsNotType<AskTimeoutException>(ex);

        Assert.Equal("hold", await hold);
    }

    // ---- Struct-reply and aggregate-reply refs expose the same overloads ----

    [Fact]
    public async Task TestStructReplyUncancelledTokenReturnsResult()
    {
        using ActorSystem asx = new();

        IActorRefStruct<ReplyActorStruct, int, int> actor = asx.SpawnStruct<ReplyActorStruct, int, int>();

        using CancellationTokenSource cts = new();
        int reply = await actor.Ask(7, cts.Token);

        Assert.Equal(7, reply);
    }

    // ---- Stress: race cancellation against drain; nothing delivered twice or double-counted ----

    [Fact]
    public async Task TestRaceCancellationAgainstDrainKeepsCountsConsistent()
    {
        using ActorSystem asx = new();

        IActorRef<ReplyActor, string, string> actor = asx.Spawn<ReplyActor, string, string>();

        const int count = 200;
        List<Task> asks = [];

        for (int i = 0; i < count; i++)
        {
            CancellationTokenSource cts = new();
            Task<string?> t = actor.Ask($"msg-{i}", cts.Token);

            // Cancel roughly half of them at a random-ish moment relative to the drain.
            if (i % 2 == 0)
                cts.Cancel();

            // Swallow cancellations; we only care that nothing corrupts the runner.
            asks.Add(t.ContinueWith(_ => { cts.Dispose(); }, TaskScheduler.Default));
        }

        await Task.WhenAll(asks);
        await asx.Wait();

        // The inbox fully drains: every message is either delivered or skipped exactly once.
        Assert.Equal(0, actor.Runner.MessageCount);
        Assert.True(actor.Runner.IsEmpty);
    }
}
