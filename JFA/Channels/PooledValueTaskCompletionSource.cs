using System.Threading.Tasks.Sources;
using JFA.Channels.Pool;

namespace JFA.Channels;

public class PooledValueTaskCompletionSource<T> : IValueTaskSource<T>,
    IThreadPoolWorkItem
{
    private ManualResetValueTaskSourceCore<T> _core = default;
    private T _response = default;
    private static readonly SloppyPool<CancellationTokenSource> _ctsPool = new SloppyPool<CancellationTokenSource>(128);

    internal static readonly SloppyPool<PooledValueTaskCompletionSource<T>>
        ResultPool = new SloppyPool<PooledValueTaskCompletionSource<T>>(128);

    private bool _noReturn;
    public bool WasCancelled { get; private set; }
    public CancellationTokenRegistration? externalCancellation { get; internal set; }
    public CancellationTokenRegistration? mainCancellation { get; private set; }
    public CancellationTokenSource? timeoutCancellation { get; internal set; }

    public T GetResult(short token)
    {
        try
        {
            return _core.GetResult(token);
        }
        finally
        {
            var shouldAddSelf = false;
            _core.Reset();
            _response = default!;
            if (mainCancellation?.Unregister() != false & externalCancellation?.Unregister() != false)
            {
                if (timeoutCancellation == null)
                {
                    shouldAddSelf = true;
                }
                else if (timeoutCancellation.TryReset() == true)
                {
                    shouldAddSelf = true;
                    _ctsPool.TryAdd(timeoutCancellation);
                }
            }

            mainCancellation = null;
            externalCancellation = null;
            timeoutCancellation = null;
            if (shouldAddSelf == true && _noReturn == false)
            {
                ResultPool.TryAdd(this);
            }
        }
    }

    ValueTaskSourceStatus IValueTaskSource<T>.GetStatus(short token)
    {
        return _core.GetStatus(token);
    }

    public void Init(CancellationToken cancellationToken = default)
    {
        if (cancellationToken.CanBeCanceled)
        {
            
            timeoutCancellation = _ctsPool.TryGet(out var ourItem)
                ? ourItem
                : new CancellationTokenSource();
            externalCancellation = cancellationToken.UnsafeRegister(static state =>
            {
                ((CancellationTokenSource)state!).Cancel();
            }, this);
            timeoutCancellation.Token.UnsafeRegister(static state =>
            {
                var inst = (PooledValueTaskCompletionSource<T>)state!;
                inst.TrySetCanceled(inst.externalCancellation!.Value.Token);
            }, this);
        }
    }

    void IValueTaskSource<T>.OnCompleted(Action<object> continuation,
        object state, short token,
        ValueTaskSourceOnCompletedFlags flags)
    {
        _core.OnCompleted(continuation, state, token, flags);
    }

    public bool TrySetResult(T result)
    {
        try
        {
            if (_noReturn == false)
            {
                _response = result;
                ThreadPool.UnsafeQueueUserWorkItem(this,
                    preferLocal: false);   
            }
            return true;
        }
        catch (Exception e)
        {
            return false;
        }
    }

    public bool TrySetCanceled(CancellationToken cancellationToken)
    {
        if (_noReturn == false)
        {
            WasCancelled = true;
            _noReturn = true;
            ThreadPool.UnsafeQueueUserWorkItem(
                state =>
                {
                    state.self._core.SetException(
                        new OperationCanceledException(
                            state.cancellationToken));
                }, (self: this, cancellationToken),
                preferLocal: false);
        }

        return false;
    }

    public ValueTask<T> AsValueTask()
    {
        return new ValueTask<T>(this, _core.Version);
    }

    public bool TrySetException(Exception exception)
    {
        if (_noReturn == false)
        {
            _noReturn = true;
            ThreadPool.UnsafeQueueUserWorkItem(
                state => { state.self._core.SetException(state.exception); },
                (self: this, exception), preferLocal: false);
            return true;
        }
        else
        {
            return false;
        }

    }

    public void Execute()
    {
        _core.SetResult(_response!);
    }
}

public static class PooledValueTaskCompletionSource
{
    public static PooledValueTaskCompletionSource<T> Get<T>(
        CancellationToken token = default)
    {
        var result = PooledValueTaskCompletionSource<T>.ResultPool.TryGet(
            out var item)
            ? item
            : new PooledValueTaskCompletionSource<T>();
        result.Init(token);
        return result;
    }

    public static void Return<T>(
        PooledValueTaskCompletionSource<T> returnInstance)
    {
        PooledValueTaskCompletionSource<T>.ResultPool.TryAdd(returnInstance);
    }
}