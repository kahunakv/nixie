using Nixie;

namespace Nixie.TrimTests;

public interface IGreeter
{
    string Greet(string name);
}

public sealed class Greeter : IGreeter
{
    public string Greet(string name) => "hello " + name;
}

/// <summary>
/// Fire-and-forget actors complete a shared signal so the host can check that the message arrived.
/// </summary>
public static class Signals
{
    public static TaskCompletionSource<string> Plain = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public static TaskCompletionSource<int> Struct = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public static TaskCompletionSource<string> Aggregate = new(TaskCreationOptions.RunContinuationsAsynchronously);
}

public sealed class PlainActor : IActor<string>
{
    public PlainActor(IActorContext<PlainActor, string> _)
    {
    }

    public Task Receive(string message)
    {
        Signals.Plain.TrySetResult(message);
        return Task.CompletedTask;
    }
}

public sealed class ReplyActor : IActor<string, string>
{
    public ReplyActor(IActorContext<ReplyActor, string, string> _)
    {
    }

    public Task<string?> Receive(string message) => Task.FromResult<string?>("reply:" + message);
}

public sealed class ReplyArgsActor : IActor<string, string>
{
    private readonly string? a;
    private readonly string? b;

    public ReplyArgsActor(IActorContext<ReplyArgsActor, string, string> _, string? a, string? b)
    {
        this.a = a;
        this.b = b;
    }

    public Task<string?> Receive(string message) => Task.FromResult<string?>($"{a ?? "null"}|{b ?? "null"}|{message}");
}

public sealed class ReplyDiActor : IActor<string, string>
{
    private readonly IGreeter greeter;
    private readonly int extra;

    public ReplyDiActor(IActorContext<ReplyDiActor, string, string> _, IGreeter greeter, int extra)
    {
        this.greeter = greeter;
        this.extra = extra;
    }

    public Task<string?> Receive(string message) => Task.FromResult<string?>(greeter.Greet(message) + extra);
}

public sealed class StructActor : IActorStruct<int>
{
    public StructActor(IActorContextStruct<StructActor, int> _)
    {
    }

    public Task Receive(int message)
    {
        Signals.Struct.TrySetResult(message);
        return Task.CompletedTask;
    }
}

public sealed class StructReplyActor : IActorStruct<int, int>
{
    private readonly int offset;

    public StructReplyActor(IActorContextStruct<StructReplyActor, int, int> _)
    {
    }

    public StructReplyActor(IActorContextStruct<StructReplyActor, int, int> _, int offset)
    {
        this.offset = offset;
    }

    public Task<int> Receive(int message) => Task.FromResult(message * 2 + offset);
}

public sealed class AggregateActor : IActorAggregate<string>
{
    public AggregateActor(IActorAggregateContext<AggregateActor, string> _)
    {
    }

    public Task Receive(List<string> messages)
    {
        Signals.Aggregate.TrySetResult(string.Join(",", messages));
        return Task.CompletedTask;
    }
}

public sealed class AggregateReplyActor : IActorAggregate<string, string>
{
    public AggregateReplyActor(IActorAggregateContext<AggregateReplyActor, string, string> _)
    {
    }

    public Task Receive(List<ActorMessageReply<string, string>> messages)
    {
        foreach (ActorMessageReply<string, string> message in messages)
            message.Promise?.SetResult("aggregate:" + message.Request);

        return Task.CompletedTask;
    }
}
