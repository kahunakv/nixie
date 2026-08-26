
using System.Collections.Concurrent;

namespace Nixie.Tests.Actors;

/// <summary>
/// Reply actor that never answers from its handler: it captures the message's pooled reply handle,
/// bypasses the delivery-loop reply, and completes the handle later from a detached task — the same
/// deferred-completion shape a consumer uses for replies that resolve after an external event.
/// Captured handles are also exported so tests can probe first-wins and staleness.
/// </summary>
public sealed class DeferredPooledReplyActor : IActor<TrySendRequest, TrySendResponse>
{
    private readonly IActorContext<DeferredPooledReplyActor, TrySendRequest, TrySendResponse> context;

    public ConcurrentQueue<ReplyHandle<TrySendResponse>> Handles { get; } = new();

    public DeferredPooledReplyActor(IActorContext<DeferredPooledReplyActor, TrySendRequest, TrySendResponse> context)
    {
        this.context = context;
    }

    public Task<TrySendResponse?> Receive(TrySendRequest message)
    {
        ReplyHandle<TrySendResponse> handle = context.Reply!.Value.PooledHandle;

        context.ByPassReply = true;
        Handles.Enqueue(handle);

        _ = Task.Run(async () =>
        {
            await Task.Delay(20);
            handle.TrySetResult(new TrySendResponse { Id = message.Id });
        });

        return Task.FromResult<TrySendResponse?>(null);
    }
}
