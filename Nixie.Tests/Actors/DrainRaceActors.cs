namespace Nixie.Tests.Actors;

public sealed class DrainRaceMessage
{
    public bool BlockUntilReleased { get; init; }

    public TaskCompletionSource? OnEnteredReceive { get; init; }

    public TaskCompletionSource? ReleaseGate { get; init; }
}

public sealed class DrainRaceActor : IActor<DrainRaceMessage>
{
    private int processedCount;

    public DrainRaceActor(IActorContext<DrainRaceActor, DrainRaceMessage> _)
    {
    }

    public int ProcessedCount => processedCount;

    public async Task Receive(DrainRaceMessage message)
    {
        message.OnEnteredReceive?.TrySetResult();

        if (message.BlockUntilReleased && message.ReleaseGate is not null)
            await message.ReleaseGate.Task.ConfigureAwait(false);

        Interlocked.Increment(ref processedCount);
    }
}

public struct DrainRaceStructMessage
{
    public bool BlockUntilReleased { get; init; }

    public TaskCompletionSource? OnEnteredReceive { get; init; }

    public TaskCompletionSource? ReleaseGate { get; init; }
}

public sealed class DrainRaceActorStruct : IActorStruct<DrainRaceStructMessage>
{
    private int processedCount;

    public DrainRaceActorStruct(IActorContextStruct<DrainRaceActorStruct, DrainRaceStructMessage> _)
    {
    }

    public int ProcessedCount => processedCount;

    public async Task Receive(DrainRaceStructMessage message)
    {
        message.OnEnteredReceive?.TrySetResult();

        if (message.BlockUntilReleased && message.ReleaseGate is not null)
            await message.ReleaseGate.Task.ConfigureAwait(false);

        Interlocked.Increment(ref processedCount);
    }
}

public sealed class DrainRaceReplyRequest
{
    public bool BlockUntilReleased { get; init; }

    public TaskCompletionSource? OnEnteredReceive { get; init; }

    public TaskCompletionSource? ReleaseGate { get; init; }
}

public sealed class DrainRaceReplyResponse
{
    public int Sequence { get; init; }
}

public sealed class DrainRaceReplyActor : IActor<DrainRaceReplyRequest, DrainRaceReplyResponse>
{
    private int processedCount;

    public DrainRaceReplyActor(IActorContext<DrainRaceReplyActor, DrainRaceReplyRequest, DrainRaceReplyResponse> _)
    {
    }

    public int ProcessedCount => processedCount;

    public async Task<DrainRaceReplyResponse?> Receive(DrainRaceReplyRequest message)
    {
        message.OnEnteredReceive?.TrySetResult();

        if (message.BlockUntilReleased && message.ReleaseGate is not null)
            await message.ReleaseGate.Task.ConfigureAwait(false);

        int sequence = Interlocked.Increment(ref processedCount);
        return new DrainRaceReplyResponse { Sequence = sequence };
    }
}

public sealed class DrainRaceAggregateActor : IActorAggregate<DrainRaceMessage>
{
    private int processedCount;

    public DrainRaceAggregateActor(IActorAggregateContext<DrainRaceAggregateActor, DrainRaceMessage> _)
    {
    }

    public int ProcessedCount => processedCount;

    public async Task Receive(List<DrainRaceMessage> messages)
    {
        foreach (DrainRaceMessage message in messages)
        {
            message.OnEnteredReceive?.TrySetResult();

            if (message.BlockUntilReleased && message.ReleaseGate is not null)
                await message.ReleaseGate.Task.ConfigureAwait(false);

            Interlocked.Increment(ref processedCount);
        }
    }
}

public sealed class ForwardReplyActor : IActor<string, string>
{
    private readonly IActorContext<ForwardReplyActor, string, string> context;
    private readonly IActorRef<ReplyActor, string, string> inner;

    public ForwardReplyActor(IActorContext<ForwardReplyActor, string, string> context)
    {
        this.context = context;
        inner = context.ActorSystem.Spawn<ReplyActor, string, string>();
    }

    public Task<string?> Receive(string message)
    {
        context.ByPassReply = true;
        inner.Send(message, context.Reply);
        return Task.FromResult<string?>(null);
    }
}

public sealed class BatchTrackingAggregateActor : IActorAggregate<string>
{
    public List<string> LastBatch { get; private set; } = [];

    public List<string> AllReceived { get; } = [];

    public int BatchInvocationCount { get; private set; }

    public TaskCompletionSource? HoldUntilReleased { get; set; }

    public TaskCompletionSource? OnEnteredReceive { get; set; }

    public BatchTrackingAggregateActor(IActorAggregateContext<BatchTrackingAggregateActor, string> _)
    {
    }

    public async Task Receive(List<string> messages)
    {
        OnEnteredReceive?.TrySetResult();

        if (HoldUntilReleased is not null)
            await HoldUntilReleased.Task.ConfigureAwait(false);

        LastBatch = [..messages];
        AllReceived.AddRange(messages);
        BatchInvocationCount++;
    }
}
