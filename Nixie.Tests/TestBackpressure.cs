
using Nixie.Tests.Actors;

namespace Nixie.Tests;

[Collection("Nixie")]
public sealed class TestBackpressure
{
    // ---- Change 1: bounded inbox / ActorBusyException ----

    [Fact]
    public async Task TestBoundedActorRejectsOverCapacityAskWithActorBusyException()
    {
        using ActorSystem asx = new();

        // capacity = 1: slow actor holds 1 message while processing; a second enqueue fills the inbox.
        // A third must be rejected.
        IActorRef<ReplySlowActor, string, string> actor =
            asx.SpawnWithOptions<ReplySlowActor, string, string>(null, new ActorRunnerOptions { MaxInboxSize = 1 });

        // Fill the inbox — this starts processing (slow, 2 s)
        Task<string?> first = actor.Ask("fill");

        await Task.Delay(50);   // drainer dequeued "fill"; actor is processing

        Task<string?> second = actor.Ask("second");   // queued; inbox depth = 1

        // Third must be rejected because inbox is at capacity
        Task<string?> third = actor.Ask("third");

        await Assert.ThrowsAsync<ActorBusyException>(async () => await third);

        // Admitted messages still complete normally
        Assert.Equal("fill", await first);
        Assert.Equal("second", await second);
    }

    [Fact]
    public async Task TestBoundedActorRejectedMessageHandlerNeverRuns()
    {
        using ActorSystem asx = new();

        IActorRef<ReplySlowActor, string, string> actor =
            asx.SpawnWithOptions<ReplySlowActor, string, string>(null, new ActorRunnerOptions { MaxInboxSize = 1 });

        actor.Send("fill");
        await Task.Delay(50);
        actor.Send("queued");

        Task<string?> rejected = actor.Ask("rejected");

        ActorBusyException ex = await Assert.ThrowsAsync<ActorBusyException>(async () => await rejected);
        Assert.Equal(1, ex.Capacity);
        Assert.Contains("rejected without processing", ex.Message);

        await asx.Wait();

        ReplySlowActor impl = (ReplySlowActor)actor.Runner.Actor!;
        Assert.Equal(0, impl.GetMessages("rejected"));
    }

    [Fact]
    public async Task TestUnboundedActorBehavesAsToday()
    {
        using ActorSystem asx = new();

        IActorRef<ReplyActor, string, string> actor = asx.Spawn<ReplyActor, string, string>();

        List<Task<string?>> tasks = [];
        for (int i = 0; i < 50; i++)
            tasks.Add(actor.Ask($"msg-{i}"));

        string?[] results = await Task.WhenAll(tasks);

        for (int i = 0; i < 50; i++)
            Assert.Equal($"msg-{i}", results[i]);
    }

    [Fact]
    public async Task TestActorBusyExceptionCarriesCorrectMetadata()
    {
        using ActorSystem asx = new();

        IActorRef<ReplySlowActor, string, string> actor =
            asx.SpawnWithOptions<ReplySlowActor, string, string>("bp-meta-actor", new ActorRunnerOptions { MaxInboxSize = 1 });

        actor.Send("fill");
        await Task.Delay(50);
        actor.Send("queued");

        Task<string?> rejected = actor.Ask("reject");
        ActorBusyException ex = await Assert.ThrowsAsync<ActorBusyException>(async () => await rejected);

        Assert.Equal("bp-meta-actor", ex.ActorName);
        Assert.Equal(1, ex.Capacity);
        Assert.True(ex.CurrentDepth >= 1);
    }

    // ---- Change 2: handler exceptions surface as exceptions, not null ----

    [Fact]
    public async Task TestThrowingHandlerSurfacesExceptionToAsker()
    {
        using ActorSystem asx = new();

        IActorRef<ThrowingReplyActor, ThrowingReplyRequest, ThrowingReplyResponse> actor =
            asx.Spawn<ThrowingReplyActor, ThrowingReplyRequest, ThrowingReplyResponse>();

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await actor.Ask(new ThrowingReplyRequest { ShouldThrow = true }));
    }

    [Fact]
    public async Task TestNullReturnIsDistinctFromException()
    {
        using ActorSystem asx = new();

        IActorRef<ThrowingReplyActor, ThrowingReplyRequest, ThrowingReplyResponse> actor =
            asx.Spawn<ThrowingReplyActor, ThrowingReplyRequest, ThrowingReplyResponse>();

        // Handler deliberately returns null — must come back as null, not an exception
        ThrowingReplyResponse? result = await actor.Ask(new ThrowingReplyRequest { Value = null, ShouldThrow = false });
        Assert.Null(result);
    }

    [Fact]
    public async Task TestNonNullReturnIsDeliveredCorrectly()
    {
        using ActorSystem asx = new();

        IActorRef<ThrowingReplyActor, ThrowingReplyRequest, ThrowingReplyResponse> actor =
            asx.Spawn<ThrowingReplyActor, ThrowingReplyRequest, ThrowingReplyResponse>();

        ThrowingReplyResponse? result = await actor.Ask(new ThrowingReplyRequest { Value = "hello", ShouldThrow = false });
        Assert.NotNull(result);
        Assert.Equal("hello", result.Value);
    }

    [Fact]
    public async Task TestExceptionInHandlerDoesNotPoisonSubsequentMessages()
    {
        using ActorSystem asx = new();

        IActorRef<ThrowingReplyActor, ThrowingReplyRequest, ThrowingReplyResponse> actor =
            asx.Spawn<ThrowingReplyActor, ThrowingReplyRequest, ThrowingReplyResponse>();

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await actor.Ask(new ThrowingReplyRequest { ShouldThrow = true }));

        ThrowingReplyResponse? result = await actor.Ask(new ThrowingReplyRequest { Value = "after-fault", ShouldThrow = false });
        Assert.NotNull(result);
        Assert.Equal("after-fault", result.Value);
    }

    // ---- Fire-n-forget bounded inbox ----

    [Fact]
    public void TestBoundedFireAndForgetThrowsActorBusyExceptionWhenFull()
    {
        using ActorSystem asx = new();

        IActorRef<FireAndForgetSlowActor, string> actor =
            asx.SpawnWithOptions<FireAndForgetSlowActor, string>(null, new ActorRunnerOptions { MaxInboxSize = 1 });

        actor.Send("fill");

        Thread.Sleep(50);   // let drainer dequeue so actor is processing; inbox is empty

        actor.Send("queued"); // inbox depth = 1

        Assert.Throws<ActorBusyException>(() => actor.Send("overflow"));
    }
}
