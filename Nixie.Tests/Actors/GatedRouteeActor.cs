
namespace Nixie.Tests.Actors;

/// <summary>
/// Routee that blocks in <see cref="Receive"/> until a shared gate is released. This freezes each routee's
/// inbox depth and keeps it in the "processing" state, so a balancing router's Step-3 (least-loaded) fallback
/// can be exercised deterministically.
/// </summary>
public sealed class GatedRouteeActor : IActor<RouterMessage>
{
    private readonly TaskCompletionSource gate;

    public GatedRouteeActor(IActorContext<GatedRouteeActor, RouterMessage> _, TaskCompletionSource gate)
    {
        this.gate = gate;
    }

    public async Task Receive(RouterMessage message) => await gate.Task;
}
