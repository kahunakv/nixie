
using Nixie.Tests.Actors;

namespace Nixie.Tests;

[Collection("Nixie")]
public sealed class TestSpawnOptionsApi
{
    // Regression for the 1.2.0 silent overload rebind: a spawn whose first constructor argument is `null`
    // must bind the params `Spawn(string?, params object[])` overload and deliver every ctor arg intact.
    // On 1.2.0 it bound `Spawn(string?, ActorRunnerOptions?, params object[])` with options=null, shifted the
    // ctor args left by one, and crashed at runtime with MissingMethodException.

    [Fact]
    public async Task TestNullFirstConstructorArgBindsParamsSpawnWithoutRebind()
    {
        using ActorSystem asx = new();

        // `null!` models a null-valued (nullable) first ctor argument; it is still null at runtime, so the
        // params overload must receive [null, "b", "c"]. The `!` only silences nullable-flow analysis.
        IActorRef<NullFirstArgActor, string, string> actor =
            asx.Spawn<NullFirstArgActor, string, string>("nfa", null!, "b", "c");

        string? reply = await actor.Ask("go");

        // [null, "b", "c"] reached the constructor — no MissingMethodException, no left-shift.
        Assert.Equal("null|b|c", reply);
    }

    [Fact]
    public async Task TestSpawnWithOptionsAppliesBoundedInboxAndControlClass()
    {
        using ActorSystem asx = new();

        IActorRef<PriorityBlockingActor, PriorityRequest, PriorityResponse> actor =
            asx.SpawnWithOptions<PriorityBlockingActor, PriorityRequest, PriorityResponse>(
                "opts", new ActorRunnerOptions
                {
                    MaxInboxSize = 1,
                    IsControlMessage = m => ((PriorityRequest)m).IsControl
                });

        // Bounded inbox is in effect: fill the single slot behind an in-flight message, then overflow.
        Task<PriorityResponse?> n0 = actor.Ask(new PriorityRequest { Id = "n0" });
        await Task.Delay(100);
        Task<PriorityResponse?> n1 = actor.Ask(new PriorityRequest { Id = "n1" });

        await Assert.ThrowsAsync<ActorBusyException>(async () =>
            await actor.Ask(new PriorityRequest { Id = "n2" }));

        // Control class is in effect: a control message is admitted despite the full normal inbox.
        actor.Send(new PriorityRequest { Id = "c0", IsControl = true });

        await Task.WhenAll(n0, n1);
        await asx.Wait();

        PriorityBlockingActor impl = (PriorityBlockingActor)actor.Runner.Actor!;
        Assert.Contains("c0", impl.ProcessedOrder);
        Assert.DoesNotContain("n2", impl.ProcessedOrder);
    }
}
