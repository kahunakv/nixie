
using System.Collections.Concurrent;

namespace Nixie.Tests.Actors;

public sealed class PriorityRequest
{
    public required string Id { get; init; }

    public bool IsControl { get; init; }
}

public sealed class PriorityResponse
{
    public string? Id { get; init; }
}

/// <summary>
/// Reply actor whose normal messages block the drainer (simulating slow work) while control messages are
/// fast. Records the global processing order so tests can assert control-overtakes-normal.
/// </summary>
public sealed class PriorityBlockingActor : IActor<PriorityRequest, PriorityResponse>
{
    public ConcurrentQueue<string> ProcessedOrder { get; } = new();

    public PriorityBlockingActor(IActorContext<PriorityBlockingActor, PriorityRequest, PriorityResponse> _) { }

    public async Task<PriorityResponse?> Receive(PriorityRequest message)
    {
        ProcessedOrder.Enqueue(message.Id);

        if (!message.IsControl)
            await Task.Delay(1500);

        return new PriorityResponse { Id = message.Id };
    }
}

/// <summary>
/// Aggregate-reply actor: normal messages block, control messages are fast. Records global processing order.
/// </summary>
public sealed class PriorityAggregateActor : IActorAggregate<PriorityRequest, PriorityResponse>
{
    public ConcurrentQueue<string> ProcessedOrder { get; } = new();

    public PriorityAggregateActor(IActorAggregateContext<PriorityAggregateActor, PriorityRequest, PriorityResponse> _) { }

    public async Task Receive(List<ActorMessageReply<PriorityRequest, PriorityResponse>> messages)
    {
        foreach (ActorMessageReply<PriorityRequest, PriorityResponse> message in messages)
        {
            ProcessedOrder.Enqueue(message.Request.Id);

            if (!message.Request.IsControl)
                await Task.Delay(500);

            message.Promise.TrySetResult(new PriorityResponse { Id = message.Request.Id });
        }
    }
}

public readonly struct PriorityStructRequest
{
    public int Id { get; init; }

    public bool IsControl { get; init; }
}

/// <summary>
/// Struct-reply actor: normal messages block, control messages are fast. Records global processing order.
/// </summary>
public sealed class PriorityStructActor : IActorStruct<PriorityStructRequest, int>
{
    public ConcurrentQueue<int> ProcessedOrder { get; } = new();

    public PriorityStructActor(IActorContextStruct<PriorityStructActor, PriorityStructRequest, int> _) { }

    public async Task<int> Receive(PriorityStructRequest message)
    {
        ProcessedOrder.Enqueue(message.Id);

        if (!message.IsControl)
            await Task.Delay(500);

        return message.Id;
    }
}

public sealed class ReadRequest
{
    public required string Kind { get; init; }   // "read" (user request) or "resume" (control completion)

    public required string Id { get; init; }

    public bool IsControl => Kind == "resume";
}

public sealed class ReadResponse
{
    public string? Id { get; init; }
}

/// <summary>
/// Models Kahuna's pattern: a user "read" defers its reply (<see cref="IActorContext{TActor,TRequest,TResponse}.ByPassReply"/>),
/// stashes the promise, and kicks off a detached "backend read" that later self-sends a control "resume"
/// which resolves the stashed promise. The self-sent resume must always be admitted (it is control) even when
/// the bounded inbox is rejecting new reads — otherwise the read's promise would be stranded forever.
/// </summary>
public sealed class SelfCompletingReadActor : IActor<ReadRequest, ReadResponse>
{
    private readonly IActorContext<SelfCompletingReadActor, ReadRequest, ReadResponse> context;

    private readonly Dictionary<string, TaskCompletionSource<ReadResponse?>> pending = new();

    public int Admitted { get; private set; }

    public int Resumed { get; private set; }

    public SelfCompletingReadActor(IActorContext<SelfCompletingReadActor, ReadRequest, ReadResponse> context)
    {
        this.context = context;
    }

    public async Task<ReadResponse?> Receive(ReadRequest message)
    {
        if (message.IsControl)
        {
            Resumed++;

            if (pending.Remove(message.Id, out TaskCompletionSource<ReadResponse?>? promise))
                promise.TrySetResult(new ReadResponse { Id = message.Id });

            return null;
        }

        // User read: defer the reply, stash the promise, schedule a detached completion.
        TaskCompletionSource<ReadResponse?> readPromise = context.Reply!.Value.Promise;
        context.ByPassReply = true;
        Admitted++;

        await Task.Delay(15);

        pending[message.Id] = readPromise;

        ReadRequest resume = new() { Kind = "resume", Id = message.Id };

        _ = Task.Run(async () =>
        {
            await Task.Delay(15);
            context.Self.Send(resume);   // control message — exempt from the bound
        });

        return null;
    }
}
