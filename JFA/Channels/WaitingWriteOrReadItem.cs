using System.Runtime.CompilerServices;
using System.Threading.Tasks.Sources;
using JFA.Channels.Pool;

namespace JFA.Channels;

internal class WaitingWriteOrReadItem : IValueTaskSource<bool>, IThreadPoolWorkItem
{
    private ManualResetValueTaskSourceCore<bool> _sourceCore = new ManualResetValueTaskSourceCore<bool>();
    private readonly SloppyPool<WaitingWriteOrReadItem>? _pool;
    private bool Result = default;
    private bool IsCtsBacked = false;
    internal WaitingWriteOrReadItem(SloppyPool<WaitingWriteOrReadItem>? pool, bool isCtsBacked)
    {
        _pool = pool;
        this.IsCtsBacked = isCtsBacked;
    }

    public void Reset()
    {
        if (_pool != null)
        {
            _pool.TryAdd(this);
        }
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool RentOrGet(SloppyPool<WaitingWriteOrReadItem> pool, out WaitingWriteOrReadItem self)
    {
        if (pool?.TryGet(out self) == true)
        {
            return true;
        }

        self = new WaitingWriteOrReadItem(pool, false);
        return false;
    }

    public static WaitingWriteOrReadItem GetNoBuffer()
    {
        return new WaitingWriteOrReadItem(null, true);
    }
    
    public bool GetResult(short token)
    {
        var res = false;
        try
        {
            res = _sourceCore.GetResult(token);
        }
        finally
        {
            if (IsCtsBacked)
            {
                _sourceCore.Reset();
                Reset();   
            }
        }

        return res;
    }

    public ValueTask<bool> AsToken()
    {
        return new ValueTask<bool>(this, _sourceCore.Version);
    }
    public ValueTaskSourceStatus GetStatus(short token)
    {
        return _sourceCore.GetStatus(token);
    }

    public void SetCancelled(CancellationToken token = default)
    {
        ThreadPool.UnsafeQueueUserWorkItem(state =>
        {
            state.self._sourceCore.SetException(new OperationCanceledException(state.token));
        }, (self: this, token), preferLocal: false);
    }

    public void SetException(Exception exception)
    {
        ThreadPool.UnsafeQueueUserWorkItem(state =>
        {
            state.self._sourceCore.SetException(state.exception);
        }, (self: this, exception), preferLocal: false);
    }

    public void SetResult(bool res)
    {
        Result = res;
        ThreadPool.UnsafeQueueUserWorkItem(this, false);
    }

    public void OnCompleted(Action<object?> continuation, object? state, short token,
        ValueTaskSourceOnCompletedFlags flags)
    {
        _sourceCore.OnCompleted(continuation, state, token, flags);
    }

    public void Execute()
    { 
        _sourceCore.SetResult(Result);
    }

}