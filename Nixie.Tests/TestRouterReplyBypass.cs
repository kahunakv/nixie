
using Nixie.Routers;
using Nixie.Tests.Actors;

namespace Nixie.Tests;

[Collection("Nixie")]
public sealed class TestRouterReplyBypass
{
    // The reply routers return a cached completed "bypass" task (value = default) but forward the real reply
    // via ByPassReply + context.Reply. These assert the caller sees the routee's real reply, not the cached
    // default — which would be null (reference) or 0 (struct) if forwarding were broken.

    [Fact]
    public async Task TestReferenceReplyRouterReturnsRealReplyNotCachedDefault()
    {
        using ActorSystem asx = new();

        IActorRef<RoundRobinActor<RouteeReplyActor, RouterMessage, RouterResponse>, RouterMessage, RouterResponse> router =
            asx.Spawn<RoundRobinActor<RouteeReplyActor, RouterMessage, RouterResponse>, RouterMessage, RouterResponse>("ref-reply-router", 3);

        RouterResponse? reply = await router.Ask(new RouterMessage(RouterMessageType.Route, "hello"));

        Assert.NotNull(reply);                 // cached bypass task carries null; null here would mean a broken forward
        Assert.Equal("hello", reply.Data);
    }

    [Fact]
    public async Task TestStructReplyRouterReturnsRealReplyNotCachedDefault()
    {
        using ActorSystem asx = new();

        IActorRefStruct<RoundRobinActorStruct<ReplyActorStruct, int, int>, int, int> router =
            asx.SpawnStruct<RoundRobinActorStruct<ReplyActorStruct, int, int>, int, int>("struct-reply-router", 3);

        int reply = await router.Ask(42);      // cached bypass task carries default(int)=0; 42 proves the real forward

        Assert.Equal(42, reply);
    }
}
