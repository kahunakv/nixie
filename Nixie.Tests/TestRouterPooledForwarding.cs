using Nixie.Routers;
using Nixie.Tests.Actors;

namespace Nixie.Tests;

[Collection("Nixie")]
public sealed class TestRouterPooledForwarding
{
    // A forwarded envelope has three kinds, and a reply router must keep the reply channel of each one.
    // An ordinary ask carries a promise. A pooled ask carries a reply handle and a null promise. A
    // fire-and-forget message carries neither. Treating "no promise" as "no reply" dropped the pooled
    // handle, and the original caller waited forever for a reply that nobody could complete.

    private static readonly TimeSpan ReplyTimeout = TimeSpan.FromSeconds(10);

    // Bounds the wait, so a dropped reply fails the test instead of hanging the suite. AsTask consumes
    // the pooled ValueTask exactly once, which is what its contract requires.
    private static Task<RouterResponse?> WithTimeout(ValueTask<RouterResponse?> reply)
    {
        return reply.AsTask().WaitAsync(ReplyTimeout);
    }

    // Polls until the actor is inside its handler with an empty inbox, so the next sends fill a known
    // number of inbox slots.
    private static async Task WaitUntilInFlight<TActor>(IActorRef<TActor, RouterMessage, RouterResponse> actor)
        where TActor : IActor<RouterMessage, RouterResponse>
    {
        for (int i = 0; i < 200; i++)
        {
            if (actor.Runner.IsProcessing && actor.Runner.MessageCount == 0)
            {
                await Task.Delay(30);
                return;
            }

            await Task.Delay(10);
        }

        throw new Xunit.Sdk.XunitException("actor never reached the in-flight state");
    }

    [Fact]
    public async Task TestPooledAskThroughRoundRobinRouterCompletes()
    {
        using ActorSystem asx = new();

        IActorRef<RoundRobinActor<RouteeReplyActor, RouterMessage, RouterResponse>, RouterMessage, RouterResponse> router =
            asx.Spawn<RoundRobinActor<RouteeReplyActor, RouterMessage, RouterResponse>, RouterMessage, RouterResponse>("pooled-rr", 1);

        Assert.True(router.TryAskPooled(new RouterMessage(RouterMessageType.Route, "hello"), out ValueTask<RouterResponse?> reply));

        RouterResponse? response = await WithTimeout(reply);

        Assert.NotNull(response);
        Assert.Equal("hello", response.Data);
    }

    [Fact]
    public async Task TestPooledAskThroughRoundRobinRouterReachesEveryRoutee()
    {
        using ActorSystem asx = new();

        IActorRef<RoundRobinActor<RouteeReplyActor, RouterMessage, RouterResponse>, RouterMessage, RouterResponse> router =
            asx.Spawn<RoundRobinActor<RouteeReplyActor, RouterMessage, RouterResponse>, RouterMessage, RouterResponse>("pooled-rr-many", 3);

        for (int i = 0; i < 30; i++)
        {
            Assert.True(router.TryAskPooled(new RouterMessage(RouterMessageType.Route, i.ToString()), out ValueTask<RouterResponse?> reply));

            RouterResponse? response = await WithTimeout(reply);

            // Each reply is the routee's own answer, and it is paired with its own request: a recycled
            // pooled promise must never answer a different ask.
            Assert.NotNull(response);
            Assert.Equal(i.ToString(), response.Data);
        }
    }

    [Fact]
    public async Task TestPooledAskThroughConsistentHashRouterCompletes()
    {
        using ActorSystem asx = new();

        IActorRef<ConsistentHashActor<RouteeReplyActor, RouterMessage, RouterResponse>, RouterMessage, RouterResponse> router =
            asx.Spawn<ConsistentHashActor<RouteeReplyActor, RouterMessage, RouterResponse>, RouterMessage, RouterResponse>("pooled-hash", 3);

        Assert.True(router.TryAskPooled(new RouterMessage(RouterMessageType.Route, "bbb"), out ValueTask<RouterResponse?> reply));

        RouterResponse? response = await WithTimeout(reply);

        Assert.NotNull(response);
        Assert.Equal("bbb", response.Data);
    }

    [Fact]
    public async Task TestPooledAskThroughTwoForwardHopsCompletes()
    {
        using ActorSystem asx = new();

        IActorRef<RouteeReplyActor, RouterMessage, RouterResponse> target =
            asx.Spawn<RouteeReplyActor, RouterMessage, RouterResponse>("hop-target");

        IActorRef<ForwardingReplyActor<RouteeReplyActor>, RouterMessage, RouterResponse> first =
            asx.Spawn<ForwardingReplyActor<RouteeReplyActor>, RouterMessage, RouterResponse>("hop-1", target);

        IActorRef<ForwardingReplyActor<ForwardingReplyActor<RouteeReplyActor>>, RouterMessage, RouterResponse> second =
            asx.Spawn<ForwardingReplyActor<ForwardingReplyActor<RouteeReplyActor>>, RouterMessage, RouterResponse>("hop-2", first);

        Assert.True(second.TryAskPooled(new RouterMessage(RouterMessageType.Route, "two-hops"), out ValueTask<RouterResponse?> reply));

        RouterResponse? response = await WithTimeout(reply);

        Assert.NotNull(response);
        Assert.Equal("two-hops", response.Data);
    }

    [Fact]
    public async Task TestOrdinaryAskThroughForwardStillCompletes()
    {
        using ActorSystem asx = new();

        IActorRef<RouteeReplyActor, RouterMessage, RouterResponse> target =
            asx.Spawn<RouteeReplyActor, RouterMessage, RouterResponse>("ordinary-target");

        IActorRef<ForwardingReplyActor<RouteeReplyActor>, RouterMessage, RouterResponse> forwarder =
            asx.Spawn<ForwardingReplyActor<RouteeReplyActor>, RouterMessage, RouterResponse>("ordinary-fwd", target);

        RouterResponse? response = await forwarder.Ask(new RouterMessage(RouterMessageType.Route, "ordinary")).WaitAsync(ReplyTimeout);

        Assert.NotNull(response);
        Assert.Equal("ordinary", response.Data);
    }

    [Fact]
    public async Task TestPromiseFreeSendThroughForwardIsStillDelivered()
    {
        using ActorSystem asx = new();

        IActorRef<RouteeReplyActor, RouterMessage, RouterResponse> target =
            asx.Spawn<RouteeReplyActor, RouterMessage, RouterResponse>("free-target");

        IActorRef<ForwardingReplyActor<RouteeReplyActor>, RouterMessage, RouterResponse> forwarder =
            asx.Spawn<ForwardingReplyActor<RouteeReplyActor>, RouterMessage, RouterResponse>("free-fwd", target);

        forwarder.Send(new RouterMessage(RouterMessageType.Route, "fire"));

        await asx.Wait();

        RouteeReplyActor routee = (RouteeReplyActor)target.Runner.Actor!;

        Assert.Equal(1, routee.GetMessages());
    }

    [Fact]
    public async Task TestPooledAskRejectedByFullTargetInboxFaults()
    {
        using ActorSystem asx = new();

        TaskCompletionSource gate = new(TaskCreationOptions.RunContinuationsAsynchronously);

        IActorRef<GatedRouterReplyActor, RouterMessage, RouterResponse> target =
            asx.SpawnWithOptions<GatedRouterReplyActor, RouterMessage, RouterResponse>(
                "busy-target", new ActorRunnerOptions { MaxInboxSize = 1 }, gate);

        IActorRef<ForwardingReplyActor<GatedRouterReplyActor>, RouterMessage, RouterResponse> forwarder =
            asx.Spawn<ForwardingReplyActor<GatedRouterReplyActor>, RouterMessage, RouterResponse>("busy-fwd", target);

        // One message occupies the handler, one fills the single inbox slot.
        target.Send(new RouterMessage(RouterMessageType.Route, "in-flight"));

        await WaitUntilInFlight(target);

        target.Send(new RouterMessage(RouterMessageType.Route, "queued"));

        Assert.True(forwarder.TryAskPooled(new RouterMessage(RouterMessageType.Route, "rejected"), out ValueTask<RouterResponse?> reply));

        // A rejected forward must complete the pooled reply, because no later delivery can complete it.
        await Assert.ThrowsAsync<ActorBusyException>(async () => await WithTimeout(reply));

        gate.TrySetResult();

        await asx.Wait();
    }

    [Fact]
    public async Task TestPooledAskToShutDownTargetCancels()
    {
        using ActorSystem asx = new();

        IActorRef<RouteeReplyActor, RouterMessage, RouterResponse> target =
            asx.Spawn<RouteeReplyActor, RouterMessage, RouterResponse>("dead-target");

        IActorRef<ForwardingReplyActor<RouteeReplyActor>, RouterMessage, RouterResponse> forwarder =
            asx.Spawn<ForwardingReplyActor<RouteeReplyActor>, RouterMessage, RouterResponse>("dead-fwd", target);

        asx.Shutdown(target);

        Assert.True(forwarder.TryAskPooled(new RouterMessage(RouterMessageType.Route, "gone"), out ValueTask<RouterResponse?> reply));

        await Assert.ThrowsAsync<TaskCanceledException>(async () => await WithTimeout(reply));
    }
}
