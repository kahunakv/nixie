
namespace Nixie.Tests.Actors;

public sealed class ReplyAggregateActor : IActorAggregate<string, string>
{
    private readonly Dictionary<string, int> receivedMessages = new();

    public ReplyAggregateActor(IActorAggregateContext<ReplyAggregateActor, string, string> _)
    {

    }

    public int GetMessages(string id)
    {
        return receivedMessages.GetValueOrDefault(id, 0);
    }

    private void IncrMessage(string id)
    {
        if (!receivedMessages.TryGetValue(id, out int value))
            receivedMessages.Add(id, 1);
        else
            receivedMessages[id] = ++value;
    }

    public Task Receive(List<ActorMessageReply<string, string>> messages)
    {
        foreach (ActorMessageReply<string, string> message in messages)
        {
            IncrMessage(message.Request);
            
            message.Promise!.SetResult(message.Request);
        }

        return Task.CompletedTask;
    }
}