
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;

namespace Nixie;

/// <summary>
/// LazyTask utility
/// </summary>
/// <typeparam name="T"></typeparam>
[AsyncMethodBuilder(typeof(LazyTaskMethodBuilder<>))]
public class LazyTask<T> : INotifyCompletion
{
    private readonly object syncObj = new();

    private T? result;

    private Exception? exception;

    private IAsyncStateMachine? asyncStateMachine;

    private Action? continuation;

    /// <summary>
    /// Constructor
    /// </summary>
    internal LazyTask()
    {
    }

    /// <summary>
    /// Async state machine get result
    /// </summary>
    /// <returns></returns>
    /// <exception cref="Exception"></exception>
    public T? GetResult()
    {
        lock (syncObj)
        {
            if (exception != null)
                ExceptionDispatchInfo.Throw(exception);

            if (!IsCompleted)
                throw new Exception("Not Completed");

            return result;
        }
    }

    /// <summary>
    /// Returns true if the task is completed
    /// </summary>
    public bool IsCompleted { get; private set; }

    /// <summary>
    /// Handler when the task is completed
    /// </summary>
    /// <param name="continuation"></param>
    public void OnCompleted(Action continuation)
    {
        IAsyncStateMachine? stateMachine;

        lock (syncObj)
        {
            stateMachine = asyncStateMachine;
            asyncStateMachine = null;

            this.continuation += continuation;
        }

        // The lazy body (MoveNext) and any continuations run outside the lock: both execute
        // arbitrary user code, which must not be invoked while holding syncObj (deadlock risk).
        stateMachine?.MoveNext();

        TryCallContinuation();
    }

    /// <summary>
    /// Returns the awaiter
    /// </summary>
    /// <returns></returns>
    public LazyTask<T> GetAwaiter() => this;

    internal void SetResult(T result)
    {
        lock (syncObj)
        {
            this.result = result;
            IsCompleted = true;
        }

        TryCallContinuation();
    }

    internal void SetException(Exception exception)
    {
        lock (syncObj)
        {
            this.exception = exception;
            IsCompleted = true;
        }

        TryCallContinuation();
    }

    internal void SetStateMachine(IAsyncStateMachine stateMachine)
    {
        // Published under the same lock OnCompleted reads it with; an unfenced write could let a
        // first await on another thread observe null and never start the lazy body.
        lock (syncObj)
        {
            asyncStateMachine = stateMachine;
        }
    }

    private void TryCallContinuation()
    {
        Action? toRun;

        // The claim is atomic under the lock (so a completer and an awaiter can both call in and
        // the continuations run exactly once), but the invocation happens outside it.
        lock (syncObj)
        {
            if (!IsCompleted || continuation == null)
                return;

            toRun = continuation;
            continuation = null;
        }

        toRun();
    }
}
