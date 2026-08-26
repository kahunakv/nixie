
using System.Collections.Concurrent;
using System.Threading.Tasks.Sources;

namespace Nixie;

/// <summary>
/// A pooled, single-use reply promise backing the <see cref="ValueTask{TResponse}"/> returned by the
/// pooled ask path (<c>TryAskPooled</c>). One instance serves one ask at a time: the runner rents it on
/// admission, exactly one completer resolves it (the delivery loop, a shutdown sweep, or a deferred
/// completer holding its <see cref="ReplyHandle{TResponse}"/>), and consuming the awaited result
/// recycles the instance back into the pool.
///
/// Correctness under recycling rests on a packed (generation, state) word: every completion must win a
/// compare-exchange on that word for its own generation, and recycling advances the generation before
/// the instance is pooled again. A completer holding a stale handle is therefore rejected instead of
/// resolving a promise that meanwhile serves a different ask. Completion is first-wins; later attempts
/// for the same generation return <c>false</c>.
///
/// The returned ValueTask must be awaited exactly once. A ValueTask that is never consumed leaves its
/// promise un-pooled; that instance is reclaimed by the garbage collector, which is safe but forfeits
/// the reuse.
/// </summary>
/// <typeparam name="TResponse"></typeparam>
public sealed class ReplyPromise<TResponse> : IValueTaskSource<TResponse?>
{
    private const int StatePending = 0;

    private const int StateCompleted = 1;

    private const int PooledLimit = 4096;

    private static readonly ConcurrentQueue<ReplyPromise<TResponse>> pool = new();

    private static int pooledCount;

    private ManualResetValueTaskSourceCore<TResponse?> core;

    // Packs (generation << 1 | state). See the class summary for the protocol.
    private long word;

    private ReplyPromise()
    {
        core.RunContinuationsAsynchronously = true;
    }

    private static long Pack(long generation, int state) => (generation << 1) | (uint)state;

    private static long GenerationOf(long packed) => packed >>> 1;

    private static int StateOf(long packed) => (int)(packed & 1);

    /// <summary>
    /// Rents a promise from the pool, or creates one when the pool is empty. The rented instance is
    /// pending for its current generation; capture <see cref="Handle"/> and <see cref="AsValueTask"/>
    /// before publishing it to any completer.
    /// </summary>
    internal static ReplyPromise<TResponse> Rent()
    {
        if (pool.TryDequeue(out ReplyPromise<TResponse>? promise))
        {
            Interlocked.Decrement(ref pooledCount);
            return promise;
        }

        return new();
    }

    /// <summary>The completion handle for the current generation.</summary>
    internal ReplyHandle<TResponse> Handle => new(this, GenerationOf(Volatile.Read(ref word)));

    /// <summary>The single-consumption awaitable for the current generation.</summary>
    internal ValueTask<TResponse?> AsValueTask() => new(this, core.Version);

    internal bool TrySetResult(long generation, TResponse? result)
    {
        if (!TryAcquireCompletion(generation))
            return false;

        core.SetResult(result);
        return true;
    }

    internal bool TrySetException(long generation, Exception exception)
    {
        if (!TryAcquireCompletion(generation))
            return false;

        core.SetException(exception);
        return true;
    }

    internal bool TrySetCanceled(long generation)
    {
        if (!TryAcquireCompletion(generation))
            return false;

        core.SetException(new TaskCanceledException());
        return true;
    }

    /// <summary>
    /// True when the promise was completed for <paramref name="generation"/>, or when that generation
    /// is already stale (the promise was recycled and serves a different ask).
    /// </summary>
    internal bool IsCompleted(long generation)
    {
        long packed = Volatile.Read(ref word);
        return GenerationOf(packed) != generation || StateOf(packed) != StatePending;
    }

    /// <summary>
    /// Claims the exclusive right to complete <paramref name="generation"/>. Exactly one caller wins;
    /// a caller with a stale generation, or one that lost the first-wins race, gets <c>false</c> and
    /// must not touch the core.
    /// </summary>
    private bool TryAcquireCompletion(long generation)
    {
        while (true)
        {
            long packed = Volatile.Read(ref word);

            if (GenerationOf(packed) != generation || StateOf(packed) != StatePending)
                return false;

            if (Interlocked.CompareExchange(ref word, Pack(generation, StateCompleted), packed) == packed)
                return true;
        }
    }

    TResponse? IValueTaskSource<TResponse?>.GetResult(short token)
    {
        try
        {
            return core.GetResult(token);
        }
        finally
        {
            // Recycle only a genuinely completed promise: a contract-violating consumption of a
            // still-pending ValueTask throws above without a completer having claimed the word, and
            // pooling that instance would hand a live completer's promise to an unrelated ask.
            if (StateOf(Volatile.Read(ref word)) == StateCompleted)
                Recycle();
        }
    }

    ValueTaskSourceStatus IValueTaskSource<TResponse?>.GetStatus(short token) => core.GetStatus(token);

    void IValueTaskSource<TResponse?>.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
        core.OnCompleted(continuation, state, token, flags);

    private void Recycle()
    {
        long generation = GenerationOf(Volatile.Read(ref word));

        core.Reset();

        // Advancing the generation invalidates every outstanding handle before the instance can be
        // rented again; only then is the instance published back to the pool.
        Volatile.Write(ref word, Pack(generation + 1, StatePending));

        if (Interlocked.Increment(ref pooledCount) <= PooledLimit)
            pool.Enqueue(this);
        else
            Interlocked.Decrement(ref pooledCount);
    }
}

/// <summary>
/// The completion side of a pooled reply promise: a (promise, generation) pair whose TrySet methods
/// resolve the promise first-wins, and silently no-op (returning <c>false</c>) once the generation is
/// stale. The default value is an absent handle whose TrySet methods all return <c>false</c>.
///
/// A handler that defers its reply (<c>ByPassReply</c>) captures this handle from the message and
/// completes it later from any thread; holding the handle after a completion attempt is harmless.
/// </summary>
/// <typeparam name="TResponse"></typeparam>
public readonly struct ReplyHandle<TResponse>
{
    private readonly ReplyPromise<TResponse>? promise;

    private readonly long generation;

    internal ReplyHandle(ReplyPromise<TResponse> promise, long generation)
    {
        this.promise = promise;
        this.generation = generation;
    }

    /// <summary>True when this is the default value (no promise attached).</summary>
    public bool IsDefault => promise is null;

    /// <summary>
    /// True when the promise was completed, or this handle is stale. An absent handle reports true.
    /// </summary>
    public bool IsCompleted => promise?.IsCompleted(generation) ?? true;

    /// <summary>Resolves the reply with a response. First completion wins; stale or repeated attempts return <c>false</c>.</summary>
    public bool TrySetResult(TResponse? result) => promise?.TrySetResult(generation, result) ?? false;

    /// <summary>Faults the reply. First completion wins; stale or repeated attempts return <c>false</c>.</summary>
    public bool TrySetException(Exception exception) => promise?.TrySetException(generation, exception) ?? false;

    /// <summary>Cancels the reply (the awaiter observes a <see cref="TaskCanceledException"/>). First completion wins.</summary>
    public bool TrySetCanceled() => promise?.TrySetCanceled(generation) ?? false;
}
