
namespace Nixie;

/// <summary>
/// Thrown when a message is rejected because the actor's inbox has reached its configured capacity.
/// The message was never enqueued and never processed, so it is unconditionally safe to retry.
/// </summary>
public class ActorBusyException : NixieException
{
    /// <summary>Name of the actor whose inbox was full.</summary>
    public string ActorName { get; }

    /// <summary>Inbox depth at the moment of rejection (approximate).</summary>
    public int CurrentDepth { get; }

    /// <summary>The configured inbox capacity.</summary>
    public int Capacity { get; }

    public ActorBusyException(string actorName, int currentDepth, int capacity)
        : base($"Actor '{actorName}' inbox is full ({currentDepth}/{capacity}); message rejected without processing, safe to retry")
    {
        ActorName = actorName;
        CurrentDepth = currentDepth;
        Capacity = capacity;
    }
}
