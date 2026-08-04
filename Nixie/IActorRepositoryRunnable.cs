
namespace Nixie;

/// <summary>
/// Represents an actor repository that can be run.
/// </summary>
public interface IActorRepositoryRunnable
{
    /// <summary>
    /// Returns true if there are pending messages to process.
    /// </summary>
    /// <returns></returns>
    public bool HasPendingMessages(out string? actorName);

    /// <summary>
    /// Returns true if the repository is processing messages.
    /// </summary>
    /// <returns></returns>
    public bool IsProcessing(out string? actorName);

    /// <summary>
    /// Signals all actors in this repository to stop accepting new messages and waits up to
    /// <paramref name="maxWait"/> for in-flight processing to drain.
    /// </summary>
    public Task GracefulShutdownAll(TimeSpan maxWait);

    /// <summary>
    /// Immediately shuts down every actor in this repository without waiting for inboxes to drain.
    /// </summary>
    public void ShutdownAll();
}
