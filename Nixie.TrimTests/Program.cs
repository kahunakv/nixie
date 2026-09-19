using Microsoft.Extensions.DependencyInjection;
using Nixie;
using Nixie.Routers;
using Nixie.TrimTests;

// Runs after a trimmed publish (PublishTrimmed=true, TrimMode=full). Exits with code 0 when every actor kind
// spawns and answers; any failure (for example a MissingMethodException from a trimmed constructor) exits non-zero.

TimeSpan timeout = TimeSpan.FromSeconds(10);
int failures = 0;

async Task Check<T>(string name, Func<Task<T>> run, T expected)
{
    try
    {
        T actual = await run().WaitAsync(timeout);

        if (EqualityComparer<T>.Default.Equals(actual, expected))
        {
            Console.WriteLine($"PASS {name}");
            return;
        }

        Console.WriteLine($"FAIL {name}: expected '{expected}', got '{actual}'");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"FAIL {name}: {ex}");
    }

    failures++;
}

// Without an IServiceProvider: the Activator.CreateInstance path.
using (ActorSystem system = new())
{
    await Check("plain", () =>
    {
        system.Spawn<PlainActor, string>("plain").Send("ping");
        return Signals.Plain.Task;
    }, "ping");

    await Check("reply", () => system.Spawn<ReplyActor, string, string>("reply").Ask("ping"), "reply:ping");

    await Check("reply with args", () =>
        system.Spawn<ReplyArgsActor, string, string>("reply-args", "a", "b").Ask("ping"), "a|b|ping");

    await Check("reply with null first arg", () =>
        system.Spawn<ReplyArgsActor, string, string>("reply-null", null!, "b").Ask("ping"), "null|b|ping");

    await Check("struct", () =>
    {
        system.SpawnStruct<StructActor, int>("struct").Send(21);
        return Signals.Struct.Task;
    }, 21);

    await Check("struct reply", () => system.SpawnStruct<StructReplyActor, int, int>("struct-reply").Ask(21), 42);

    await Check("struct reply with args", () =>
        system.SpawnStruct<StructReplyActor, int, int>("struct-reply-args", 1).Ask(21), 43);

    await Check("aggregate", () =>
    {
        system.SpawnAggregate<AggregateActor, string>("aggregate").Send("ping");
        return Signals.Aggregate.Task;
    }, "ping");

    await Check("aggregate reply", () =>
        system.SpawnAggregate<AggregateReplyActor, string, string>("aggregate-reply").Ask("ping"), "aggregate:ping");

    await Check("with options", () =>
        system.SpawnWithOptions<ReplyActor, string, string>("reply-options", new ActorRunnerOptions()).Ask("ping"), "reply:ping");

    await Check("round robin router", () =>
        system.Spawn<RoundRobinActor<ReplyActor, string, string>, string, string>("router", 3).Ask("ping"), "reply:ping");
}

// With an IServiceProvider: the ActivatorUtilities.CreateInstance path.
ServiceCollection services = new();
services.AddSingleton<IGreeter, Greeter>();

using (ServiceProvider provider = services.BuildServiceProvider())
using (ActorSystem system = new(provider))
{
    await Check("di reply", () => system.Spawn<ReplyActor, string, string>("reply").Ask("ping"), "reply:ping");

    await Check("di reply with service and args", () =>
        system.Spawn<ReplyDiActor, string, string>("reply-di", 7).Ask("world"), "hello world7");
    await Check("di struct reply", () => system.SpawnStruct<StructReplyActor, int, int>("struct-reply").Ask(21), 42);

    await Check("di aggregate reply", () =>
        system.SpawnAggregate<AggregateReplyActor, string, string>("aggregate-reply").Ask("ping"), "aggregate:ping");
}

Console.WriteLine(failures == 0 ? "All trim checks passed." : $"{failures} trim check(s) failed.");

return failures == 0 ? 0 : 1;
