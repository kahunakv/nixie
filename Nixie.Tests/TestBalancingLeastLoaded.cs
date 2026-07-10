
using Nixie.Routers;
using Nixie.Tests.Actors;

namespace Nixie.Tests;

[Collection("Nixie")]
public sealed class TestBalancingLeastLoaded
{
    // Step 3 (least-loaded fallback) is reached only when every routee is both processing and non-empty,
    // so Steps 1/2 (idle / empty routee) are skipped. The gate keeps every routee blocked and its depth
    // frozen, making the selection deterministic.

    [Fact]
    public async Task TestBalancingFallbackRoutesToLeastLoaded()
    {
        using ActorSystem asx = new();
        TaskCompletionSource gate = new(TaskCreationOptions.RunContinuationsAsynchronously);

        // After each routee's drainer picks up its first message, depths become [4, 8, 2, 6] — min at index 2.
        int[] sends = [5, 9, 3, 7];
        List<IActorRef<GatedRouteeActor, RouterMessage>> routees = SpawnGatedRoutees(asx, gate, "ll", sends);

        await Task.Delay(400);   // let every routee dequeue its first message and block on the gate

        // Precondition: all busy + non-empty, distinct depths [4, 8, 2, 6].
        Assert.Equal(4, routees[0].Runner.MessageCount);
        Assert.Equal(8, routees[1].Runner.MessageCount);
        Assert.Equal(2, routees[2].Runner.MessageCount);
        Assert.Equal(6, routees[3].Runner.MessageCount);

        IActorRef<BalancingActor<GatedRouteeActor, RouterMessage>, RouterMessage> router =
            asx.Spawn<BalancingActor<GatedRouteeActor, RouterMessage>, RouterMessage>("bal-ll", routees);
        router.Send(new RouterMessage(RouterMessageType.Route, "routed"));

        await Task.Delay(300);   // router forwards to the least-loaded routee

        // Only the least-loaded routee (index 2) received the routed message.
        Assert.Equal(3, routees[2].Runner.MessageCount);   // 2 -> 3
        Assert.Equal(4, routees[0].Runner.MessageCount);
        Assert.Equal(8, routees[1].Runner.MessageCount);
        Assert.Equal(6, routees[3].Runner.MessageCount);

        gate.SetResult();
        await asx.Wait();
    }

    [Fact]
    public async Task TestBalancingFallbackTieGoesToFirstMinimum()
    {
        using ActorSystem asx = new();
        TaskCompletionSource gate = new(TaskCreationOptions.RunContinuationsAsynchronously);

        // Depths after pickup: [2, 2, 4] — a tie for the minimum between index 0 and index 1.
        int[] sends = [3, 3, 5];
        List<IActorRef<GatedRouteeActor, RouterMessage>> routees = SpawnGatedRoutees(asx, gate, "tie", sends);

        await Task.Delay(400);

        // Precondition: depths [2, 2, 4] — a tie for the minimum between index 0 and index 1.
        Assert.Equal(2, routees[0].Runner.MessageCount);
        Assert.Equal(2, routees[1].Runner.MessageCount);
        Assert.Equal(4, routees[2].Runner.MessageCount);

        IActorRef<BalancingActor<GatedRouteeActor, RouterMessage>, RouterMessage> router =
            asx.Spawn<BalancingActor<GatedRouteeActor, RouterMessage>, RouterMessage>("bal-tie", routees);
        router.Send(new RouterMessage(RouterMessageType.Route, "routed"));

        await Task.Delay(300);

        // Strict `<` keeps the first minimum: index 0 wins the tie, not index 1.
        Assert.Equal(3, routees[0].Runner.MessageCount);
        Assert.Equal(2, routees[1].Runner.MessageCount);
        Assert.Equal(4, routees[2].Runner.MessageCount);

        gate.SetResult();
        await asx.Wait();
    }

    [Fact]
    public async Task TestBalancingFallbackSingleRoutee()
    {
        using ActorSystem asx = new();
        TaskCompletionSource gate = new(TaskCreationOptions.RunContinuationsAsynchronously);

        List<IActorRef<GatedRouteeActor, RouterMessage>> routees = SpawnGatedRoutees(asx, gate, "one", [3]);

        await Task.Delay(400);
        Assert.Equal(2, routees[0].Runner.MessageCount);

        IActorRef<BalancingActor<GatedRouteeActor, RouterMessage>, RouterMessage> router =
            asx.Spawn<BalancingActor<GatedRouteeActor, RouterMessage>, RouterMessage>("bal-one", routees);
        router.Send(new RouterMessage(RouterMessageType.Route, "routed"));

        await Task.Delay(300);

        Assert.Equal(3, routees[0].Runner.MessageCount);   // the only routee gets it

        gate.SetResult();
        await asx.Wait();
    }

    private static List<IActorRef<GatedRouteeActor, RouterMessage>> SpawnGatedRoutees(
        ActorSystem asx, TaskCompletionSource gate, string prefix, int[] sends)
    {
        List<IActorRef<GatedRouteeActor, RouterMessage>> routees = new(sends.Length);

        for (int i = 0; i < sends.Length; i++)
        {
            IActorRef<GatedRouteeActor, RouterMessage> routee =
                asx.Spawn<GatedRouteeActor, RouterMessage>($"{prefix}-{i}", gate);

            for (int m = 0; m < sends[i]; m++)
                routee.Send(new RouterMessage(RouterMessageType.Route, "x"));

            routees.Add(routee);
        }

        return routees;
    }
}
