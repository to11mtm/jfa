using System.Collections.Concurrent;
using System.Threading.Channels;
using JFA.Scheduler;

namespace JFA.Channels;

public class ChannelUtils
{
    public class ChannelFanOutter<TIn,TPartition,TPartitionState,TPartitionHandler>
    {
        private class HandlerAndState
        {
            public readonly TPartitionHandler Handler;
            public readonly TPartitionState State;
            private readonly AsyncMutex _mutex = new AsyncMutex(); 
            public HandlerAndState(TPartitionHandler handler, TPartitionState state)
            {
                Handler = handler;
                State = state;
            }

            public Task EnterAsync(CancellationToken token)
            {
                return _mutex.EnterAsync(token);
            }

            public void Exit()
            {
                _mutex.Exit();
            }
        }
        private readonly ConcurrentDictionary<TPartition, HandlerAndState>
            _handlers = new();

        private readonly Func<TIn, TPartition> _partition;
        private readonly Func<TIn, TPartition, TPartitionHandler> _newHandler;
        private readonly Func<TIn, TPartition, TPartitionState> _newState;
        private readonly Func<TPartitionHandler, TPartitionState, TIn, ValueTask> _newItem;
        private readonly Func<TPartitionHandler, Exception, TPartitionState, ValueTask> _onFault;

        private readonly
            Func<TPartitionHandler, TIn, Exception, TPartitionState, ValueTask>
            _onItemError;
        
        private readonly Func<TPartitionHandler, TPartitionState, ValueTask> _onComplete;

        public ChannelFanOutter(
            Func<TIn,TPartition> partition, 
            Func<TIn,TPartition, TPartitionHandler> newHandler,
            Func<TIn,TPartition, TPartitionState> newState,
            Func<TPartitionHandler, TPartitionState, TIn, ValueTask> newItem,
            Func<TPartitionHandler, Exception, TPartitionState, ValueTask> onError,
            Func<TPartitionHandler, TPartitionState, ValueTask> onComplete
            )
        {
            _partition = partition;
            _newHandler = newHandler;
            _newState = newState;
            _newItem = newItem;
            _onFault = onError;
            _onComplete = onComplete;
        }

        private int _state = 0;
        public async ValueTask Complete()
        {
            Interlocked.And(ref _state, 2);
            closeAll();
        }

        private async ValueTask closeAll()
        {
            foreach (var k in _handlers.Keys)
            {
                _handlers.TryRemove(k, out _);
            }
        }

        public async ValueTask<bool> AddAndProcess(TIn item,
            CancellationToken token = default)
        {
            if (Volatile.Read(ref _state) != 0)
            {
                return false;
            }
            var key = _partition(item);
            //TODO: Make better AFA Thread safety, not re-initting zombie state on close.
            var ph = _handlers.GetOrAdd(key, static (p, args) =>
            {
                var (fo, k) = args;
                return new HandlerAndState(fo._newHandler(k, p),
                    fo._newState(k, p));
            }, (this, item));
            if (Interlocked.And(ref _state, 0) == 0)
            {
                await ph.EnterAsync(token);
                try
                {
                    await _newItem(ph.Handler, ph.State, item);
                    return true;
                }
                catch (Exception e)
                {
                    await _onItemError(ph.Handler, item, e, ph.State);
                }
                finally
                {
                    ph.Exit();
                }
            }

            return false;
        }
    }
    public static readonly Exception DoneWritingSentinel = new("DONEWRITING");

    public sealed class BufferedItemHolder<T>
    {
        public BufferedItemHolder(T item)
        {
            this.Item = item;
        }

        public T Item { get; private set; }

        public BufferedItemHolder<T> Swap(T newItem)
        {
            this.Item = newItem;
            return this;
        }
    }
}