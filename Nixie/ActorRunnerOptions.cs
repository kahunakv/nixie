
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
    /// </summary>
    public int? MaxInboxSize { get; init; }
}
