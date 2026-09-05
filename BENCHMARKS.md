# Benchmarks

This document reports the message throughput of the Nixie actor types and routers.

## How To Run

```shell
dotnet run --project Nixie.Benchmarks -c Release -- throughput [floodN] [askN] [routees]
```

The defaults are 2,000,000 flood messages, 100,000 asks, and 4 routees per router.
The benchmark source is `Nixie.Benchmarks/Throughput.cs`.

The same project also contains an allocation benchmark. Run it without an argument:

```shell
dotnet run --project Nixie.Benchmarks -c Release
```

## Test Conditions

| Item | Value |
| --- | --- |
| Processor | Apple Arm64, 8 logical cores |
| Runtime | .NET 8.0.8 |
| Garbage collector | Workstation, non-concurrent |
| Handler | One counter increment per message |
| Trials | 2 |

Each actor increments a padded per-instance counter. The harness sums every counter after the drain.
Every scenario below processed the exact number of messages that it sent.

## Fire-And-Forget Throughput

One producer thread sends 2,000,000 messages. The elapsed time covers the admission and the drain.

| Actor type | Trial 1 (msg/s) | Trial 2 (msg/s) | ns/msg |
| --- | --- | --- | --- |
| Struct actor, `Send` | 20,075,564 | 21,462,198 | ~48 |
| Reply actor, `TrySend` | 17,820,452 | 17,848,426 | ~56 |
| Aggregate actor, `Send` | 16,124,221 | 17,580,547 | ~59 |
| Class actor, `Send` | 14,108,860 | 14,690,091 | ~70 |

The struct actor is the fastest actor type. Its message never boxes.

## Router Throughput

One producer thread sends 2,000,000 messages through a router with 4 routees.

| Router | Trial 1 (msg/s) | Trial 2 (msg/s) |
| --- | --- | --- |
| Consistent hash, struct message | 5,820,790 | 3,839,369 |
| Consistent hash, class message | 4,531,661 | 3,678,850 |
| Round robin, struct message | 3,255,525 | 4,003,631 |
| Round robin, class message | 3,217,611 | 2,971,272 |
| Balancing, class message | 2,883,326 | 2,630,855 |

A router costs about 4 times the direct rate. The router is itself an actor, so each message takes two hops.

The spread between the two trials is wide. The gap between the round-robin router and the
consistent-hash router is inside that spread. Do not treat that gap as real without more trials.

## Many Producer Threads

Eight producer threads send 2,000,000 messages in total.

| Scenario | Trial 1 (msg/s) | Trial 2 (msg/s) |
| --- | --- | --- |
| Single class actor | 5,289,469 | 4,838,326 |
| Round-robin router, 4 routees | 3,427,054 | 4,224,177 |
| Consistent-hash router, 4 routees | 3,437,924 | 3,785,812 |

More producer threads lower the single-actor rate. The inbox becomes a contention point.

## Request And Response Round Trips

The harness sends 100,000 asks. A batch of 64 in flight issues 64 asks, then awaits all of them.

| Scenario | Trial 1 (msg/s) | Trial 2 (msg/s) | Latency |
| --- | --- | --- | --- |
| Aggregate actor, 64 in flight | 3,129,205 | 3,090,980 | — |
| Class actor, 64 in flight | 2,351,409 | 2,551,483 | — |
| Round-robin router, 64 in flight | 1,454,878 | 1,454,799 | — |
| Struct actor, one at a time | 555,493 | 625,212 | ~1.7 µs |
| Class actor, `TryAskPooled`, one at a time | 519,241 | 548,481 | ~1.9 µs |
| Class actor, `Ask`, one at a time | 514,341 | 557,454 | ~1.9 µs |

A serial ask measures latency, not a throughput ceiling. The actor wakes once per message.

The aggregate actor leads the pipelined rows. It replies to a whole batch per call.

## Limits Of These Numbers

1. Each flood admits every message before the drain finishes, so the inbox grows large.
   The rate is end to end. It is not a steady-state rate under a paced load.
2. The handler does no work. A real handler hides part of the framework cost.
3. Two trials are not enough for a small difference. Repeat the run on your own hardware.
