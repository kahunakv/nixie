namespace Nixie.Tests.Actors;

/// <summary>
/// Forwards every message to a target actor the way a reply router does: it marks its own reply as
/// bypassed and hands its envelope to the target, so the target replies to the original caller.
/// The target actor type is a type parameter, so two of these can be chained to test several hops.
/// </summary>
/// <typeparam name="TTarget"></typeparam>
public sealed class ForwardingReplyActor<TTarget> : IActor<RouterMessage, RouterResponse>
    where TTarget : IActor<RouterMessage, RouterResponse>
{
    // The real reply is forwarded to the target; this completed task only satisfies the signature.
    private static readonly Task<RouterResponse?> BypassResult = Task.FromResult<RouterResponse?>(default);

    private readonly IActorContext<ForwardingReplyActor<TTarget>, RouterMessage, RouterResponse> context;

    private readonly IActorRef<TTarget, RouterMessage, RouterResponse> target;

    public ForwardingReplyActor(
        IActorContext<ForwardingReplyActor<TTarget>, RouterMessage, RouterResponse> context,
        IActorRef<TTarget, RouterMessage, RouterResponse> target)
    {
        this.context = context;
        this.target = target;
    }

    public Task<RouterResponse?> Receive(RouterMessage message)
    {
        context.ByPassReply = true;
        target.Send(message, context.Reply);

        return BypassResult;
    }
}

/// <summary>
/// Reply actor that blocks in Receive until a shared gate is released, so a bounded inbox behind it
/// can be filled deterministically.
/// </summary>
public sealed class GatedRouterReplyActor : IActor<RouterMessage, RouterResponse>
{
    private readonly TaskCompletionSource gate;

    public GatedRouterReplyActor(IActorContext<GatedRouterReplyActor, RouterMessage, RouterResponse> _, TaskCompletionSource gate)
    {
        this.gate = gate;
    }

    public async Task<RouterResponse?> Receive(RouterMessage message)
    {
        await gate.Task;

        return new RouterResponse(message.Data);
    }
}
