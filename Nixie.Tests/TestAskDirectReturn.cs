
using Nixie.Tests.Actors;

namespace Nixie.Tests;

[Collection("Nixie")]
public sealed class TestAskDirectReturn
{
    // The simple Ask overloads are non-async and return the promise's task directly. A synchronous fault
    // raised inside SendAndTryDeliver (e.g. a throwing control-message predicate) must still be delivered
    // through the returned task — never thrown synchronously from the Ask call itself.

    [Fact]
    public async Task TestSynchronousControlPredicateFaultSurfacesThroughAskTask()
    {
        using ActorSystem asx = new();

        IActorRef<ReplyActor, string, string> actor =
            asx.SpawnWithOptions<ReplyActor, string, string>("pred-throw", new ActorRunnerOptions
            {
                IsControlMessage = _ => throw new InvalidOperationException("predicate boom")
            });

        // Must not throw synchronously: obtaining the task succeeds even though classification faults.
        Task<string?> task = actor.Ask("hi");

        // The fault is observed only on await, matching the previous async behavior.
        InvalidOperationException ex =
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await task);
        Assert.Equal("predicate boom", ex.Message);
    }

    [Fact]
    public async Task TestSimpleAskStillReturnsReply()
    {
        using ActorSystem asx = new();

        IActorRef<ReplyActor, string, string> actor = asx.Spawn<ReplyActor, string, string>();

        Assert.Equal("echo", await actor.Ask("echo"));
    }
}
