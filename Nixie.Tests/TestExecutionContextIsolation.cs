
using Nixie.Tests.Actors;

namespace Nixie.Tests;

[Collection("Nixie")]
public sealed class TestExecutionContextIsolation
{
    internal static readonly AsyncLocal<string?> Ambient = new();

    // ---- The drain loop must not inherit the triggering sender's ExecutionContext: the wakeup is
    // scheduled with ThreadPool.UnsafeQueueUserWorkItem, so an AsyncLocal set by the sender is not
    // visible inside the actor's handler (previously Task.Run flowed it, and only from whichever
    // sender happened to trigger the wakeup) ----

    [Fact]
    public async Task TestDrainLoopDoesNotInheritSenderAsyncLocal()
    {
        using ActorSystem asx = new();

        IActorRef<AmbientCaptureActor, TrySendRequest, TrySendResponse> actor =
            asx.Spawn<AmbientCaptureActor, TrySendRequest, TrySendResponse>("ec-isolation");

        Ambient.Value = "sender-context";

        try
        {
            TrySendResponse? response = await actor.Ask(new TrySendRequest { Id = "m1" });
            Assert.Equal("m1", response!.Id);

            AmbientCaptureActor impl = (AmbientCaptureActor)actor.Runner.Actor!;
            Assert.Null(impl.Observed);

            // The sender's own context is untouched by the send.
            Assert.Equal("sender-context", Ambient.Value);
        }
        finally
        {
            Ambient.Value = null;
        }
    }
}

/// <summary>
/// Records the value of <see cref="TestExecutionContextIsolation.Ambient"/> observed inside the handler.
/// </summary>
public sealed class AmbientCaptureActor : IActor<TrySendRequest, TrySendResponse>
{
    public string? Observed { get; private set; } = "unset";

    public AmbientCaptureActor(IActorContext<AmbientCaptureActor, TrySendRequest, TrySendResponse> _)
    {
    }

    public Task<TrySendResponse?> Receive(TrySendRequest message)
    {
        Observed = TestExecutionContextIsolation.Ambient.Value;
        return Task.FromResult<TrySendResponse?>(new TrySendResponse { Id = message.Id });
    }
}
