
namespace Nixie.Tests.Actors;

public sealed class PongAggregateActor : IActorAggregate<string, string>
{
    public PongAggregateActor(IActorAggregateContext<PongAggregateActor, string, string> _)
    {

    }

    public Task Receive(List<ActorMessageReply<string, string>> messages)
    {
        foreach (ActorMessageReply<string, string> x in messages)
            x.Promise!.SetResult(x.Request);

        return Task.CompletedTask;
    }
}

public sealed class PingAggregateActor : IActorAggregate<string, string>
{
    private readonly IActorRefAggregate<PongAggregateActor, string, string> pongRef;

    private int receivedMessages;

    public PingAggregateActor(IActorAggregateContext<PingAggregateActor, string, string> context)
    {
        pongRef = context.ActorSystem.SpawnAggregate<PongAggregateActor, string, string>();
    }

    public int GetMessages()
    {
        return receivedMessages;
    }

    private void IncrMessage()
    {
        receivedMessages++;
    }

    public async Task Receive(List<ActorMessageReply<string, string>> messages)
    {
        IncrMessage();

        foreach (ActorMessageReply<string, string> x in messages)
        {
            string? pongReply = await pongRef.Ask(x.Request, TimeSpan.FromSeconds(2));
            x.Promise!.SetResult(pongReply);
        }
    }
}