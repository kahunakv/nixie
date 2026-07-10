
namespace Nixie.Tests.Actors;

/// <summary>
/// Actor whose first constructor argument after the context is a nullable reference type. Used to pin the
/// 1.2.0 overload-rebind regression: <c>Spawn("name", null, "b", "c")</c> must bind the params overload and
/// deliver <c>[null, "b", "c"]</c> as constructor args, not silently rebind to a options-carrying overload.
/// </summary>
public sealed class NullFirstArgActor : IActor<string, string>
{
    private readonly string? a;
    private readonly string? b;
    private readonly string? c;

    public NullFirstArgActor(
        IActorContext<NullFirstArgActor, string, string> _,
        string? a,
        string? b,
        string? c)
    {
        this.a = a;
        this.b = b;
        this.c = c;
    }

    public Task<string?> Receive(string message)
        => Task.FromResult<string?>($"{a ?? "null"}|{b ?? "null"}|{c ?? "null"}");
}
