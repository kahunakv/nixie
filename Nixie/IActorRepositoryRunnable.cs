
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
    /// Returns true if any actor has pending messages or is processing one, and names the first actor of
    /// each kind. Pending messages take precedence: when an actor has them, no processing name is reported.
    ///
    /// A caller that polls for quiescence uses this instead of the two checks above, because one scan
    /// answers both questions. The default implementation keeps the two separate scans, so an existing
    /// implementation of this interface stays valid; a repository that can scan once overrides it.
    /// </summary>
    /// <param name="pendingActorName"></param>
    /// <param name="processingActorName"></param>
    /// <returns></returns>
    public bool HasActivity(out string? pendingActorName, out string? processingActorName)
    {
        if (HasPendingMessages(out pendingActorName))
        {
            processingActorName = null;
            return true;
        }

        return IsProcessing(out processingActorName);
    }

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
