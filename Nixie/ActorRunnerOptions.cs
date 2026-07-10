
namespace Nixie;

/// <summary>
/// Configuration options for an actor runner. Pass to <c>Spawn</c> / <c>SpawnAggregate</c> / <c>SpawnStruct</c>
/// overloads to opt in to bounded-inbox backpressure.
/// </summary>
public sealed class ActorRunnerOptions
{
    /// <summary>
    /// Maximum number of messages that may be queued in the inbox.
    /// When the inbox reaches this depth, additional messages are rejected with
    /// <see cref="ActorBusyException"/> without being enqueued (safe to retry).
    /// <c>null</c> (the default) preserves the original unbounded behavior.
    /// Control messages (see <see cref="IsControlMessage"/>) are exempt from this bound.
    /// </summary>
    public int? MaxInboxSize { get; init; }

    /// <summary>
    /// Optional predicate that classifies a message as a priority <em>control</em> message.
    /// Control messages are <b>never</b> rejected by <see cref="MaxInboxSize"/> (they are exempt from the
    /// bound) and are delivered <b>ahead</b> of ordinary messages, so a completion that resolves an
    /// already-admitted request can always get through and overtake a backlog of new requests.
    /// FIFO is preserved within each class; global cross-class FIFO is intentionally not preserved.
    /// <c>null</c> (the default) means every message is ordinary — behavior is unchanged.
    /// Only the reply, struct-reply, and aggregate-reply runners honor this; fire-and-forget runners ignore it.
    /// </summary>
    public Func<object, bool>? IsControlMessage { get; init; }
}
