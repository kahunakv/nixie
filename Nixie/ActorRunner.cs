
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Nixie;

/// <summary>
/// Represents a runner for coordinating the lifecycle and message delivery for a specific actor instance.
/// This class manages an actor's message queue and controls the processing state and shutdown behavior.
/// </summary>
/// <typeparam name="TActor">The type of the actor that implements the <see cref="IActor{TRequest}"/> interface.</typeparam>
/// <typeparam name="TRequest">The type of the message that the actor processes.</typeparam>
public sealed class ActorRunner<TActor, TRequest> where TActor : IActor<TRequest> where TRequest : class
{
    private readonly ActorSystem actorSystem;

    private readonly ILogger? logger;

    private readonly int? maxInboxSize;

    private readonly ConcurrentQueue<ActorMessage<TRequest>> inbox = new();

    private int pendingMessageCount;

    private TaskCompletionSource? gracefulShutdown;

    private int processing = 1;

    private int shutdown = 1;

    /// <summary>
    /// Returns the name of the actor
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Returns true if the actor's inbox is empty
    /// </summary>
    public bool IsEmpty => inbox.IsEmpty;

    /// <summary>
    /// Returns the number of messages in the inbox
    /// </summary>
    public int MessageCount => Volatile.Read(ref pendingMessageCount);

    /// <summary>
    /// Reference to the actual actor
    /// </summary>
    public IActor<TRequest>? Actor { get; set; }

    /// <summary>
    /// Reference to the current actor context
    /// </summary>
    public ActorContext<TActor, TRequest>? ActorContext { get; set; }

    /// <summary>
    /// Returns true if the runner is processing messages
    /// </summary>
    public bool IsProcessing => processing == 0;

    /// <summary>
    /// Returns true if the actor is shutdown
    /// </summary>
    public bool IsShutdown => shutdown == 0;

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="actorSystem"></param>
    /// <param name="logger"></param>
    /// <param name="name"></param>
    public ActorRunner(ActorSystem actorSystem, ILogger? logger, string name, int? maxInboxSize = null)
    {
        this.actorSystem = actorSystem;
        this.logger = logger;
        this.maxInboxSize = maxInboxSize;

        Name = name;
    }

    /// <summary>
    /// Enqueues a message to the actor and tries to deliver it.
    /// </summary>
    /// <param name="message"></param>
    /// <param name="sender"></param>
    public void SendAndTryDeliver(TRequest message, IGenericActorRef? sender)
    {
        if (shutdown == 0)
            return;

        if (maxInboxSize.HasValue)
        {
            int newCount = Interlocked.Increment(ref pendingMessageCount);
            if (newCount > maxInboxSize.Value)
            {
                Interlocked.Decrement(ref pendingMessageCount);
                throw new ActorBusyException(Name, newCount - 1, maxInboxSize.Value);
            }
        }
        else
        {
            Interlocked.Increment(ref pendingMessageCount);
        }

        inbox.Enqueue(new(message, sender));

        if (1 == Interlocked.Exchange(ref processing, 0))
            _ = DeliverMessages();

        /*if (1 == Interlocked.Exchange(ref processing, 0))
        {
            if (inbox.IsEmpty)
                _ = DeliverSingleMessage(new(message, sender));
            else
            {
                inbox.Enqueue(new(message, sender));
                
                _ = DeliverMessages();
            }
        }
        else
            inbox.Enqueue(new(message, sender));*/
    }

    /// <summary>
    /// Try to shutdown the actor and returns a bool indicating success
    /// </summary>
    /// <returns></returns>
    public bool Shutdown()
    {
        bool success = 1 == Interlocked.Exchange(ref shutdown, 0);

        if (success)
            ActorContext?.PostShutdown();

        return success;
    }

    /// <summary>
    /// Tries to shutdown the actor returns a task whose result confirms shutdown within the specified timespan
    /// </summary>
    /// <param name="maxWait"></param>
    /// <returns></returns>
    public async ValueTask<bool> GracefulShutdown(TimeSpan maxWait)
    {
        if (inbox.IsEmpty)
            return Shutdown();

        if (gracefulShutdown is not null)
            return false;

        gracefulShutdown = new(TaskCreationOptions.RunContinuationsAsynchronously);

        Task timeout = Task.Delay(maxWait);

        Task completed = await Task.WhenAny(
            timeout,
            gracefulShutdown.Task
        );

        if (completed == timeout)
            Shutdown();

        return completed != timeout;
    }

    /// <summary>
    /// Enqueues a message to the actor and tries to deliver it.
    /// The request/response type actors use an object to assign the response once completed.    
    /// </summary>
    /// <returns></returns>
    private async Task DeliverMessages()
    {
        try
        {
            if (Actor is null || ActorContext is null || shutdown == 0)
            {
                gracefulShutdown?.SetResult();
                return;
            }

            ActorContext.Runner = this;

            while (shutdown == 1)
            {
                while (inbox.TryDequeue(out ActorMessage<TRequest> message))
                {
                    Interlocked.Decrement(ref pendingMessageCount);

                    if (shutdown == 0)
                        break;

                    if (ActorContext is not null)
                    {
                        // last sender is assigned
                        if (message.Sender is not null)
                            ActorContext.Sender = message.Sender;
                        else
                            ActorContext.Sender = (IGenericActorRef)actorSystem.Nobody;
                    }

                    try
                    {
                        await Actor.Receive(message.Request);
                    }
                    catch (Exception ex)
                    {
                        logger?.LogError("[{Actor}] {Exception}: {Message}\n{StackTrace}", Name, ex.GetType().Name, ex.Message, ex.StackTrace);
                    }
                }

                Interlocked.Exchange(ref processing, 1);

                if (inbox.IsEmpty || shutdown == 0)
                    break;

                if (Interlocked.Exchange(ref processing, 0) == 1)
                    continue;

                break;
            }

            gracefulShutdown?.SetResult();
        }
        catch (Exception ex)
        {
            logger?.LogError("[{Actor}] {Exception}: {Message}\n{StackTrace}", Name, ex.GetType().Name, ex.Message, ex.StackTrace);
        }
    }

    /// <summary>
    /// Processes a single message from the actor's inbox, ensuring proper delivery to the actor,
    /// and handles conditions such as shutdown or errors during message processing.
    /// </summary>
    /// <param name="message">The message to be delivered, encapsulating the request and sender information.</param>
    private async Task DeliverSingleMessage(ActorMessage<TRequest> message)
    {
        try
        {
            if (Actor is null || ActorContext is null || shutdown == 0)
            {
                gracefulShutdown?.SetResult();
                return;
            }

            ActorContext.Runner = this;

            //await DeliverMessageInternal(Actor, message);
            
            if (ActorContext is not null)
            {
                // last sender is assigned
                if (message.Sender is not null)
                    ActorContext.Sender = message.Sender;
                else
                    ActorContext.Sender = (IGenericActorRef)actorSystem.Nobody;
            }

            try
            {
                await Actor.Receive(message.Request);
            }
            catch (Exception ex)
            {
                logger?.LogError("[{Actor}] {Exception}: {Message}\n{StackTrace}", Name, ex.GetType().Name, ex.Message, ex.StackTrace);
                
                await System.IO.File.WriteAllTextAsync("/tmp/error.log", ex.ToString());
            }

            do
            {
                while (inbox.TryDequeue(out message))
                {
                    if (shutdown == 0)
                        break;

                    //await DeliverMessageInternal(Actor, message);
                    
                    if (ActorContext is not null)
                    {
                        // last sender is assigned
                        if (message.Sender is not null)
                            ActorContext.Sender = message.Sender;
                        else
                            ActorContext.Sender = (IGenericActorRef)actorSystem.Nobody;
                    }

                    try
                    {
                        await Actor.Receive(message.Request);
                    }
                    catch (Exception ex)
                    {
                        logger?.LogError("[{Actor}] {Exception}: {Message}\n{StackTrace}", Name, ex.GetType().Name, ex.Message, ex.StackTrace);
                        
                        await System.IO.File.WriteAllTextAsync("/tmp/error.log", ex.ToString());
                    }
                }
            } 
            while (shutdown == 1 && Interlocked.CompareExchange(ref processing, 1, 0) != 0);

            gracefulShutdown?.SetResult();
        }
        catch (Exception ex)
        {
            logger?.LogError("[{Actor}] {Exception}: {Message}\n{StackTrace}", Name, ex.GetType().Name, ex.Message, ex.StackTrace);
            
            await System.IO.File.WriteAllTextAsync("/tmp/error.log", ex.ToString());
        }
    }

    /// <summary>
    /// Delivers a message to the actor for processing.
    /// Ensures proper context assignment and handles exceptions during actor message processing.
    /// </summary>
    /// <param name="actor">The actor that will process the message.</param>
    /// <param name="message">The message to be processed by the actor.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private async Task DeliverMessageInternal(IActor<TRequest> actor, ActorMessage<TRequest> message)
    {
        if (ActorContext is not null)
        {
            // last sender is assigned
            if (message.Sender is not null)
                ActorContext.Sender = message.Sender;
            else
                ActorContext.Sender = (IGenericActorRef)actorSystem.Nobody;
        }

        try
        {
            await actor.Receive(message.Request);
        }
        catch (Exception ex)
        {
            logger?.LogError("[{Actor}] {Exception}: {Message}\n{StackTrace}", Name, ex.GetType().Name, ex.Message, ex.StackTrace);
        }
    }
    
    /// <summary>
    /// Allows to peek at the next message in the inbox without removing it.
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public bool TryPeek(out TRequest? message)
    {
        if (inbox.TryPeek(out ActorMessage<TRequest> nextMssage))
        {
            message = nextMssage.Request;
            return true;
        }

        message = null;
        return false;
    }
    
    /// <summary>
    /// Allows to dequeue the next message in the inbox.
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public bool TryDequeue(out TRequest? message)
    {
        if (inbox.TryDequeue(out ActorMessage<TRequest> nextMssage))
        {
            Interlocked.Decrement(ref pendingMessageCount);
            message = nextMssage.Request;
            return true;
        }

        message = null;
        return false;
    }
}
