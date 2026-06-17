
namespace Nixie.Tests.Actors;

public sealed class ThrowingReplyRequest
{
    public bool ShouldThrow { get; init; }
    public string? Value { get; init; }
}

public sealed class ThrowingReplyResponse
{
    public string? Value { get; init; }
}

public sealed class ThrowingReplyActor : IActor<ThrowingReplyRequest, ThrowingReplyResponse>
{
    private int processedCount;

    public ThrowingReplyActor(IActorContext<ThrowingReplyActor, ThrowingReplyRequest, ThrowingReplyResponse> _) { }

    public int ProcessedCount => processedCount;

    public Task<ThrowingReplyResponse?> Receive(ThrowingReplyRequest message)
    {
        processedCount++;

        if (message.ShouldThrow)
            throw new InvalidOperationException("deliberate handler fault");

        if (message.Value is null)
            return Task.FromResult<ThrowingReplyResponse?>(null);

        return Task.FromResult<ThrowingReplyResponse?>(new ThrowingReplyResponse { Value = message.Value });
    }
}
