
using System.Collections.Concurrent;

namespace Nixie.Tests.Actors;

public sealed class TrySendRequest
{
    public required string Id { get; init; }

    public bool IsControl { get; init; }
}

public sealed class TrySendResponse
{
    public string? Id { get; init; }
}

public readonly struct TrySendStructRequest
{
    public int Id { get; init; }

    public bool IsControl { get; init; }
}

/// <summary>
/// Reply actor whose first (normal) message blocks the drainer on a shared gate, so a test can fill the
/// bounded inbox behind it deterministically. Control messages never block. Records processing order.
/// </summary>
public sealed class GatedReplyActor : IActor<TrySendRequest, TrySendResponse>
{
    private readonly TaskCompletionSource gate;

    public ConcurrentQueue<string> Processed { get; } = new();

    public GatedReplyActor(IActorContext<GatedReplyActor, TrySendRequest, TrySendResponse> _, TaskCompletionSource gate)
    {
        this.gate = gate;
    }

    public async Task<TrySendResponse?> Receive(TrySendRequest message)
    {
        if (!message.IsControl)
            await gate.Task;

        Processed.Enqueue(message.Id);

        return new TrySendResponse { Id = message.Id };
    }
}

/// <summary>
/// Struct-reply variant of <see cref="GatedReplyActor"/>.
/// </summary>
public sealed class GatedStructActor : IActorStruct<TrySendStructRequest, int>
{
    private readonly TaskCompletionSource gate;

    public ConcurrentQueue<int> Processed { get; } = new();

    public GatedStructActor(IActorContextStruct<GatedStructActor, TrySendStructRequest, int> _, TaskCompletionSource gate)
    {
        this.gate = gate;
    }

    public async Task<int> Receive(TrySendStructRequest message)
    {
        if (!message.IsControl)
            await gate.Task;

        Processed.Enqueue(message.Id);

        return message.Id;
    }
}

/// <summary>
/// Aggregate-reply variant of <see cref="GatedReplyActor"/>. The first batch (a single normal message) blocks
/// the drainer on the gate; control messages processed in a later batch never block.
/// </summary>
public sealed class GatedAggregateActor : IActorAggregate<TrySendRequest, TrySendResponse>
{
    private readonly TaskCompletionSource gate;

    public ConcurrentQueue<string> Processed { get; } = new();

    public GatedAggregateActor(IActorAggregateContext<GatedAggregateActor, TrySendRequest, TrySendResponse> _, TaskCompletionSource gate)
    {
        this.gate = gate;
    }

    public async Task Receive(List<ActorMessageReply<TrySendRequest, TrySendResponse>> messages)
    {
        foreach (ActorMessageReply<TrySendRequest, TrySendResponse> message in messages)
        {
            if (!message.Request.IsControl)
                await gate.Task;

            Processed.Enqueue(message.Request.Id);

            // Promise is null for a fire-and-forget TrySend message; only Ask-delivered messages get a reply.
            message.Promise?.TrySetResult(new TrySendResponse { Id = message.Request.Id });
        }
    }
}
