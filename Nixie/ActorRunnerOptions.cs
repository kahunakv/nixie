
namespace Nixie;

/// <summary>
/// Configuration options for an actor runner. Pass to <c>Spawn</c> / <c>SpawnAggregate</c> / <c>SpawnStruct</c>
/// (via the <c>*WithOptions</c> overloads) to opt in to bounded-inbox backpressure and a priority control class.
/// For struct-request actors, prefer the generic <see cref="ActorRunnerOptions{TRequest}"/> to avoid boxing
/// each request during control-message classification.
/// </summary>
public class ActorRunnerOptions
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
    /// <para>
    /// The request is passed as <see cref="object"/>. For a reference-type request this is a free reference
    /// conversion, but a struct request is <b>boxed</b> on every call. Struct actors that want allocation-free
    /// classification should use <see cref="ActorRunnerOptions{TRequest}"/> instead.
    /// </para>
    /// </summary>
    public Func<object, bool>? IsControlMessage { get; init; }
}

/// <summary>
/// Strongly-typed <see cref="ActorRunnerOptions"/> whose control-message classifier receives the request as
/// its concrete <typeparamref name="TRequest"/> type. This avoids boxing a struct request on every
/// classification. It derives from <see cref="ActorRunnerOptions"/>, so it is accepted anywhere an
/// <see cref="ActorRunnerOptions"/> is — the existing <c>*WithOptions</c> spawn methods take it as-is.
/// When set, this typed predicate takes precedence over the base <see cref="ActorRunnerOptions.IsControlMessage"/>.
/// </summary>
/// <typeparam name="TRequest">The actor's request type.</typeparam>
public sealed class ActorRunnerOptions<TRequest> : ActorRunnerOptions
{
    /// <summary>
    /// Typed priority control-message classifier. Semantics match
    /// <see cref="ActorRunnerOptions.IsControlMessage"/> but the request is passed as <typeparamref name="TRequest"/>,
    /// so a struct request is not boxed. When non-<c>null</c>, this is used in preference to the base predicate.
    /// </summary>
    public new Func<TRequest, bool>? IsControlMessage { get; init; }
}
