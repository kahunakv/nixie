# Nixie

A lightweight, strongly typed actor model implementation for C#/.NET.

[![Run Tests](https://github.com/andresgutierrez/nixie/actions/workflows/run-tests.yml/badge.svg)](https://github.com/andresgutierrez/nixie/actions/workflows/run-tests.yml)
[![NuGet](https://img.shields.io/nuget/v/Nixie.svg?style=flat-square)](https://www.nuget.org/packages/Nixie)
[![Nuget](https://img.shields.io/nuget/dt/Nixie)](https://www.nuget.org/packages/Nixie)

## Overview

Nixie is a small actor framework built on the .NET Task Parallel Library. It focuses on compile-time type safety, nullable reference type support, simple actor lifecycle management, and fast in-process message passing.

Actors process messages asynchronously and encapsulate their own state. Callers communicate with actors through typed actor references, using either fire-and-forget `Send` or request/response `Ask`.

## Features

- **Strongly typed actors:** request and response types are expressed in actor interfaces and actor references.
- **Fire-and-forget actors:** implement `IActor<TRequest>` and receive messages with `Send`.
- **Request/response actors:** implement `IActor<TRequest, TResponse>` and receive messages with `Ask` or `Send`.
- **Struct message support:** use `IActorStruct<TRequest>` and `IActorStruct<TRequest, TResponse>` with `SpawnStruct` to avoid reference-type messages.
- **Aggregate actors:** use `IActorAggregate<TRequest>` or `IActorAggregate<TRequest, TResponse>` to process queued messages in batches.
- **Routers:** round-robin and consistent-hash routers are available for class and struct actors.
- **Actor lookup:** named actors can be resolved later with `Get` or `GetStruct`.
- **Actor shutdown:** actors can be stopped by name or reference, immediately or through graceful shutdown.
- **Timers and scheduling:** schedule one-time messages, periodic messages, actor shutdowns, and stop timers.
- **Sender context:** actors can pass sender references and reply through the sender.
- **Actor context:** actors receive access to `Self`, `Sender`, `ActorSystem`, `Logger`, and shutdown hooks.
- **Dependency injection:** actors can be constructed through `IServiceProvider`, including additional spawn arguments.
- **Logging:** pass an `ILogger` to `ActorSystem` for framework logging.
- **Wait support:** `ActorSystem.Wait()` waits until current actor queues finish processing.
- **Nullable enabled:** the library is built with nullable reference types enabled.

## Requirements

- .NET SDK 8.0 or later
- C# nullable reference types are recommended

The package targets `net8.0`.

## Installation

Using the .NET CLI:

```shell
dotnet add package Nixie --version 1.2.2
```

Using the NuGet Package Manager Console:

```shell
Install-Package Nixie -Version 1.2.2
```

## Basic Usage

```csharp
using Nixie;

public sealed class GreetMessage
{
    public string Greeting { get; }

    public GreetMessage(string greeting)
    {
        Greeting = greeting;
    }
}

public sealed class GreeterActor : IActor<GreetMessage>
{
    public Task Receive(GreetMessage message)
    {
        Console.WriteLine("Message: {0}", message.Greeting);
        return Task.CompletedTask;
    }
}

using ActorSystem system = new();

IActorRef<GreeterActor, GreetMessage> greeter =
    system.Spawn<GreeterActor, GreetMessage>();

greeter.Send(new GreetMessage("Hello, Nixie!"));

await system.Wait();
```

## Actor Types

### Fire-And-Forget Actors

Implement `IActor<TRequest>` when the actor processes messages without returning a response.

```csharp
public sealed class CounterActor : IActor<string>
{
    private int count;

    public Task Receive(string message)
    {
        count++;
        return Task.CompletedTask;
    }
}

IActorRef<CounterActor, string> counter =
    system.Spawn<CounterActor, string>("counter");

counter.Send("increment");
```

### Request/Response Actors

Implement `IActor<TRequest, TResponse>` when callers need a response.

```csharp
public sealed class EchoActor : IActor<string, string>
{
    public Task<string?> Receive(string message)
    {
        return Task.FromResult<string?>(message);
    }
}

IActorRef<EchoActor, string, string> echo =
    system.Spawn<EchoActor, string, string>();

string? reply = await echo.Ask("hello", TimeSpan.FromSeconds(2));
```

`Ask` supports overloads with a timeout and with an explicit sender. When the timeout is reached, Nixie throws `AskTimeoutException`.

### Struct Actors

Use struct actors for value-type request and response messages.

```csharp
public readonly record struct Add(int Left, int Right);
public readonly record struct Sum(int Value);

public sealed class CalculatorActor : IActorStruct<Add, Sum>
{
    public Task<Sum> Receive(Add message)
    {
        return Task.FromResult(new Sum(message.Left + message.Right));
    }
}

IActorRefStruct<CalculatorActor, Add, Sum> calculator =
    system.SpawnStruct<CalculatorActor, Add, Sum>();

Sum sum = await calculator.Ask(new Add(2, 3));
```

### Aggregate Actors

Aggregate actors receive batches instead of individual messages. They are useful when many queued messages can be processed more efficiently together.

```csharp
public sealed class BatchActor : IActorAggregate<string>
{
    public Task Receive(List<string> messages)
    {
        Console.WriteLine("Batch size: {0}", messages.Count);
        return Task.CompletedTask;
    }
}

IActorRefAggregate<BatchActor, string> batch =
    system.SpawnAggregate<BatchActor, string>();

batch.Send("one");
batch.Send("two");
await system.Wait();
```

Aggregate request/response actors implement `IActorAggregate<TRequest, TResponse>` and receive `List<ActorMessageReply<TRequest, TResponse>>`.

## Actor Context

Actors can ask for a typed context in their constructor. Nixie injects the context automatically.

```csharp
public sealed class ParentActor : IActor<string>
{
    private readonly IActorContext<ParentActor, string> context;
    private readonly IActorRef<ChildActor, string> child;

    public ParentActor(IActorContext<ParentActor, string> context)
    {
        this.context = context;
        child = context.ActorSystem.Spawn<ChildActor, string>();
    }

    public Task Receive(string message)
    {
        child.Send(message, context.Self);
        return Task.CompletedTask;
    }
}
```

Context properties include:

- `ActorSystem`: the owning actor system.
- `Self`: a reference to the current actor.
- `Sender`: the sender reference, when one was supplied.
- `Logger`: the logger passed to `ActorSystem`, if any.
- `OnPostShutdown`: event invoked when the actor is shut down.

Request/response contexts also expose `Reply` and `ByPassReply` for advanced response handling.

## Spawning And Lookup

Actors can be unnamed or named. Named actors are unique per actor type within an actor system.

```csharp
IActorRef<CounterActor, string> counter =
    system.Spawn<CounterActor, string>("counter");

IActorRef<CounterActor, string>? existing =
    system.Get<CounterActor, string>("counter");
```

Spawning a second actor with the same actor type and name throws `NixieException`.

You can pass additional constructor arguments after the optional name:

```csharp
IActorRef<ConfiguredActor, string> actor =
    system.Spawn<ConfiguredActor, string>(null, 100, "mode-a");
```

## Dependency Injection

Pass an `IServiceProvider` to `ActorSystem` to let Nixie resolve actor constructor dependencies.

```csharp
using Microsoft.Extensions.DependencyInjection;
using Nixie;

IServiceCollection services = new ServiceCollection();
services.AddSingleton<IMyService, MyService>();

using ServiceProvider provider = services.BuildServiceProvider();
using ActorSystem system = new(provider);

IActorRef<MyActor, string> actor = system.Spawn<MyActor, string>();
```

DI can be combined with spawn arguments. Nixie provides the actor context, resolves registered services, and uses the extra arguments supplied to `Spawn`.

## Routers

Router extension methods live in the `Nixie.Routers` namespace.

```csharp
using Nixie.Routers;

IActorRef<RoundRobinActor<WorkerActor, WorkItem>, WorkItem> router =
    system.CreateRoundRobinRouter<WorkerActor, WorkItem>("workers", instances: 4);

router.Send(new WorkItem("job-1"));
```

Round-robin routers distribute messages across routees in order. You can create routees automatically by passing an instance count, or pass an existing list of actor references.

```csharp
List<IActorRef<WorkerActor, WorkItem>> workers = new()
{
    system.Spawn<WorkerActor, WorkItem>(),
    system.Spawn<WorkerActor, WorkItem>()
};

IActorRef<RoundRobinActor<WorkerActor, WorkItem>, WorkItem> router =
    system.CreateRoundRobinRouter("workers", workers);
```

Consistent-hash routers require request messages to implement `IConsistentHashable`.

```csharp
public sealed class WorkItem : IConsistentHashable
{
    public string Key { get; }

    public WorkItem(string key)
    {
        Key = key;
    }

    public int GetHash()
    {
        return Key.GetHashCode();
    }
}

IActorRef<ConsistentHashActor<WorkerActor, WorkItem>, WorkItem> router =
    system.CreateConsistentHashRouter<WorkerActor, WorkItem>("hashed-workers", 4);
```

Struct router variants are available through `CreateRoundRobinRouterStruct` and `CreateConsistentHashRouterStruct`.

## Timers And Scheduling

Schedule a message once:

```csharp
system.ScheduleOnce(counter, "increment", TimeSpan.FromSeconds(1));
```

Start and stop a periodic timer:

```csharp
system.StartPeriodicTimer(
    counter,
    "counter-timer",
    "increment",
    initialDelay: TimeSpan.Zero,
    interval: TimeSpan.FromSeconds(1));

system.StopPeriodicTimer(counter, "counter-timer");
```

Stop all timers for an actor:

```csharp
system.StopAllTimers(counter);
```

Schedule actor shutdown:

```csharp
system.ScheduleShutdown(counter, TimeSpan.FromMinutes(5));
```

Struct actor timer variants are available through `ScheduleOnceStruct` and `StartPeriodicTimerStruct`.

## Shutdown

Actors can be shut down by name or by reference.

```csharp
bool stoppedByName = system.Shutdown<CounterActor, string>("counter");
bool stoppedByRef = system.Shutdown(counter);
```

Graceful shutdown waits until the actor confirms shutdown within the specified timeout.

```csharp
bool stopped = await system.GracefulShutdown(counter, TimeSpan.FromSeconds(5));
```

Struct actors use `ShutdownStruct` and `GracefulShutdownStruct`.

Actors can subscribe to post-shutdown cleanup through context:

```csharp
public sealed class CleanupActor : IActor<string>
{
    public CleanupActor(IActorContext<CleanupActor, string> context)
    {
        context.OnPostShutdown += () => Console.WriteLine("cleaned up");
    }

    public Task Receive(string message) => Task.CompletedTask;
}
```

## Waiting For Work

`ActorSystem.Wait()` waits until currently known repositories have no pending messages and no actor is processing.

```csharp
actor.Send("message");
await system.Wait();
```

This is useful in tests, command-line tools, and controlled shutdown flows.

## Logging

Pass an `ILogger` to the actor system:

```csharp
using Microsoft.Extensions.Logging;

using ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
{
    builder.AddConsole();
});

using ActorSystem system = new(logger: loggerFactory.CreateLogger("Nixie"));
```

The logger is also available to actors through their context.

## LazyTask

Nixie includes `LazyTask<T>` and `LazyTaskMethodBuilder` support for lazily started async methods.

```csharp
private async LazyTask<MyResult> CreateResultAsync()
{
    await Task.Delay(100);
    return new MyResult();
}

LazyTask<MyResult> task = CreateResultAsync();
MyResult result = await task;
```

## Development

Clone the repository and run tests:

```shell
dotnet test
```

The solution contains:

- `Nixie`: the actor framework.
- `Nixie.Tests`: xUnit tests covering actors, replies, routers, scheduling, DI, shutdown, logging, hashing, and `LazyTask`.

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

Nixie is released under the MIT License. See [LICENSE](LICENSE).

## Name Origin

[Nixies](https://en.wikipedia.org/wiki/Nixie_(folklore)) are shapeshifting water spirits in Germanic folklore.
