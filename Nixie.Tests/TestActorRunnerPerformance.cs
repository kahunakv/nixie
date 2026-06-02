using Nixie.Tests.Actors;

namespace Nixie.Tests;

[Collection("Nixie")]
public sealed class TestActorRunnerPerformance
{
    private const int DrainRaceIterations = 250;

    [Fact]
    public async Task DrainRaceStressFireAndForgetActor()
    {
        using ActorSystem asx = new();
        IActorRef<DrainRaceActor, DrainRaceMessage> actor = asx.Spawn<DrainRaceActor, DrainRaceMessage>();
        int expectedProcessed = 0;

        for (int i = 0; i < DrainRaceIterations; i++)
        {
            TaskCompletionSource enteredReceive = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource releaseGate = new(TaskCreationOptions.RunContinuationsAsynchronously);

            actor.Send(new DrainRaceMessage
            {
                BlockUntilReleased = true,
                OnEnteredReceive = enteredReceive,
                ReleaseGate = releaseGate,
            });

            await enteredReceive.Task.WaitAsync(TimeSpan.FromSeconds(5));

            actor.Send(new DrainRaceMessage { BlockUntilReleased = false });
            releaseGate.TrySetResult();
            expectedProcessed += 2;

            await asx.Wait();
            Assert.Equal(expectedProcessed, ((DrainRaceActor)actor.Runner.Actor!).ProcessedCount);
        }
    }

    [Fact]
    public async Task DrainRaceStressReplyActor()
    {
        using ActorSystem asx = new();
        IActorRef<DrainRaceReplyActor, DrainRaceReplyRequest, DrainRaceReplyResponse> actor =
            asx.Spawn<DrainRaceReplyActor, DrainRaceReplyRequest, DrainRaceReplyResponse>();
        int expectedProcessed = 0;

        for (int i = 0; i < DrainRaceIterations; i++)
        {
            TaskCompletionSource enteredReceive = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource releaseGate = new(TaskCreationOptions.RunContinuationsAsynchronously);

            Task<DrainRaceReplyResponse?> first = actor.Ask(new DrainRaceReplyRequest
            {
                BlockUntilReleased = true,
                OnEnteredReceive = enteredReceive,
                ReleaseGate = releaseGate,
            });

            await enteredReceive.Task.WaitAsync(TimeSpan.FromSeconds(5));

            Task<DrainRaceReplyResponse?> second = actor.Ask(new DrainRaceReplyRequest { BlockUntilReleased = false });
            releaseGate.TrySetResult();

            DrainRaceReplyResponse? firstResponse = await first.WaitAsync(TimeSpan.FromSeconds(5));
            DrainRaceReplyResponse? secondResponse = await second.WaitAsync(TimeSpan.FromSeconds(5));

            Assert.NotNull(firstResponse);
            Assert.NotNull(secondResponse);
            expectedProcessed += 2;

            await asx.Wait();
            Assert.Equal(expectedProcessed, ((DrainRaceReplyActor)actor.Runner.Actor!).ProcessedCount);
        }
    }

    [Fact]
    public async Task DrainRaceStressStructActor()
    {
        using ActorSystem asx = new();
        IActorRefStruct<DrainRaceActorStruct, DrainRaceStructMessage> actor =
            asx.SpawnStruct<DrainRaceActorStruct, DrainRaceStructMessage>();
        int expectedProcessed = 0;

        for (int i = 0; i < DrainRaceIterations; i++)
        {
            TaskCompletionSource enteredReceive = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource releaseGate = new(TaskCreationOptions.RunContinuationsAsynchronously);

            actor.Send(new DrainRaceStructMessage
            {
                BlockUntilReleased = true,
                OnEnteredReceive = enteredReceive,
                ReleaseGate = releaseGate,
            });

            await enteredReceive.Task.WaitAsync(TimeSpan.FromSeconds(5));

            actor.Send(new DrainRaceStructMessage { BlockUntilReleased = false });
            releaseGate.TrySetResult();
            expectedProcessed += 2;

            await asx.Wait();
            Assert.Equal(expectedProcessed, ((DrainRaceActorStruct)actor.Runner.Actor!).ProcessedCount);
        }
    }

    [Fact]
    public async Task DrainRaceStressAggregateActor()
    {
        using ActorSystem asx = new();
        IActorRefAggregate<DrainRaceAggregateActor, DrainRaceMessage> actor =
            asx.SpawnAggregate<DrainRaceAggregateActor, DrainRaceMessage>();
        int expectedProcessed = 0;

        for (int i = 0; i < DrainRaceIterations; i++)
        {
            TaskCompletionSource enteredReceive = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource releaseGate = new(TaskCreationOptions.RunContinuationsAsynchronously);

            actor.Send(new DrainRaceMessage
            {
                BlockUntilReleased = true,
                OnEnteredReceive = enteredReceive,
                ReleaseGate = releaseGate,
            });

            await enteredReceive.Task.WaitAsync(TimeSpan.FromSeconds(5));

            actor.Send(new DrainRaceMessage { BlockUntilReleased = false });
            releaseGate.TrySetResult();
            expectedProcessed += 2;

            await asx.Wait();
            Assert.Equal(expectedProcessed, ((DrainRaceAggregateActor)actor.Runner.Actor!).ProcessedCount);
        }
    }

    [Fact]
    public async Task ParentReplyForwardingCompletesCallerPromise()
    {
        using ActorSystem asx = new();
        IActorRef<ForwardReplyActor, string, string> actor = asx.Spawn<ForwardReplyActor, string, string>();

        string? reply = await actor.Ask("forwarded");
        Assert.Equal("forwarded", reply);
        await asx.Wait();
    }

    [Fact]
    public async Task AggregateActorReceivesQueuedMessagesWithoutLoss()
    {
        using ActorSystem asx = new();
        IActorRefAggregate<BatchTrackingAggregateActor, string> actor =
            asx.SpawnAggregate<BatchTrackingAggregateActor, string>();

        const int messageCount = 128;
        BatchTrackingAggregateActor batchActor = (BatchTrackingAggregateActor)actor.Runner.Actor!;
        TaskCompletionSource enteredReceive = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseBatch = new(TaskCreationOptions.RunContinuationsAsynchronously);
        batchActor.OnEnteredReceive = enteredReceive;
        batchActor.HoldUntilReleased = releaseBatch;

        actor.Send("0");
        await enteredReceive.Task.WaitAsync(TimeSpan.FromSeconds(5));

        for (int i = 1; i < messageCount; i++)
            actor.Send(i.ToString());

        releaseBatch.TrySetResult();
        await asx.Wait();

        Assert.Equal(messageCount, batchActor.AllReceived.Count);
        Assert.Equal(2, batchActor.BatchInvocationCount);
        Assert.Equal(messageCount - 1, batchActor.LastBatch.Count);

        for (int i = 0; i < messageCount; i++)
            Assert.Contains(i.ToString(), batchActor.AllReceived);
    }

    [Fact]
    public async Task MessageCountTracksEnqueueProcessingAndTryDequeue()
    {
        using ActorSystem asx = new();
        IActorRef<DrainRaceActor, DrainRaceMessage> actor = asx.Spawn<DrainRaceActor, DrainRaceMessage>("message-count");

        Assert.Equal(0, actor.Runner.MessageCount);

        TaskCompletionSource enteredReceive = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseGate = new(TaskCreationOptions.RunContinuationsAsynchronously);

        actor.Send(new DrainRaceMessage
        {
            BlockUntilReleased = true,
            OnEnteredReceive = enteredReceive,
            ReleaseGate = releaseGate,
        });

        await enteredReceive.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(0, actor.Runner.MessageCount);

        actor.Send(new DrainRaceMessage { BlockUntilReleased = false });
        actor.Send(new DrainRaceMessage { BlockUntilReleased = false });
        Assert.Equal(2, actor.Runner.MessageCount);

        Assert.True(actor.Runner.TryDequeue(out DrainRaceMessage? dequeued));
        Assert.NotNull(dequeued);
        Assert.Equal(1, actor.Runner.MessageCount);

        releaseGate.TrySetResult();
        await asx.Wait();
        Assert.Equal(0, actor.Runner.MessageCount);
    }

    [Fact]
    public async Task MessageCountNeverNegativeUnderConcurrentSends()
    {
        using ActorSystem asx = new();
        IActorRef<FireAndForgetActor, string> actor = asx.Spawn<FireAndForgetActor, string>("concurrent-count");

        const int senders = 16;
        const int messagesPerSender = 50;
        const string messageId = "concurrent-race";
        Task[] sendTasks = new Task[senders];

        for (int s = 0; s < senders; s++)
        {
            sendTasks[s] = Task.Run(() =>
            {
                for (int m = 0; m < messagesPerSender; m++)
                {
                    actor.Send(messageId);
                    if (actor.Runner.MessageCount < 0)
                        throw new InvalidOperationException("MessageCount went negative");
                }
            });
        }

        await Task.WhenAll(sendTasks);
        await asx.Wait();

        Assert.Equal(0, actor.Runner.MessageCount);
        Assert.Equal(senders * messagesPerSender, ((FireAndForgetActor)actor.Runner.Actor!).GetMessages(messageId));
    }
}
