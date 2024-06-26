using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using JFA.Channels.Pool;

namespace JFA.Channels;

public class BatchingChannel<TIn, TBatch> : Channel<TIn,TBatch>
{
    private static readonly SloppyPool<WaitingWriteOrReadItem> _readerPool =
        new SloppyPool<WaitingWriteOrReadItem>(32);

    private static readonly SloppyPool<WaitingWriteOrReadItem> _writerPool =
        new SloppyPool<WaitingWriteOrReadItem>(32);
    private readonly object _lockObj = new();
    private readonly bool _ordered;
    private readonly ConcurrentQueue<TBatch> _q = new();
    //todo: use MRVTSC instead of TCS
    private readonly ConcurrentQueue<TaskCompletionSource<bool>>
        _waitingForWaitToWrite = new();
    private readonly ConcurrentQueue<TaskCompletionSource<bool>>
        _waitingForWaitToRead = new();
    
    private Exception? _errorException = null;
    private ChannelUtils.BufferedItemHolder<TBatch>? _item = null;

    private readonly TaskCompletionSource _taskCompletionSource =
        new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

    private readonly int? _maxBuffer;
    private readonly Func<TIn, TBatch, TBatch> _add;
    private readonly Func<TIn, TBatch> _seed;
    private int _currentSize;
    private readonly int _maxSize;
    private readonly Func<TIn, int> _cost;

    public BatchingChannel(int maxSize,
        Func<TIn, int> cost,
        Func<TIn, TBatch> seed,
        Func<TIn, TBatch, TBatch> add,
        int? maxBuffer = null,
        bool ordered = true)
    {
        this._maxSize = maxSize;
        this._currentSize = 0;
        this._cost = cost;
        this._seed = seed;
        this._add = add;
        this._maxBuffer = maxBuffer; 
        this.Reader = new BatchingChannelReader(this);
        this.Writer = new BatchingChannelWriter(this);
        maxBuffer = maxBuffer ?? Environment.ProcessorCount;
        _ordered = ordered;
    }

    protected class BatchingChannelWriter : ChannelWriter<TIn>
    {
        private readonly BatchingChannel<TIn, TBatch> _parent;
        public BatchingChannelWriter(BatchingChannel<TIn, TBatch> parent)
        {
            _parent = parent;
        }

        public override bool TryComplete(Exception? error = null)
        {
            lock (_parent)
            {
                if (_parent._errorException != null)
                {
                    return false;
                }
                else
                {
                    if (error == null)
                    {
                        _parent._errorException = ChannelUtils.DoneWritingSentinel;
                    }
                    else
                    {
                        _parent._errorException = error;
                    }
                    while (_parent._waitingForWaitToWrite.TryDequeue(out var tcs))
                    {
                        if (error == null)
                        {
                            tcs.TrySetResult(false);
                        }
                        else
                        {
                            tcs.TrySetException(error);
                        }
                    }
                    if (_parent._q.IsEmpty && _parent._item == null)
                    {
                        _parent._taskCompletionSource.TrySetResult();
                        while (_parent._waitingForWaitToRead.TryDequeue(out var tcs))
                        {
                            if (error == null)
                            {
                                tcs.TrySetResult(false);
                            }
                            else
                            {
                                tcs.TrySetException(error);
                            }
                        }
                    }
                    else
                    {
                        if (_parent._waitingForWaitToRead.IsEmpty == false)
                        {
                            //SHOULD NOT GET HERE IN CURRENT IMPL
                            while (_parent._waitingForWaitToRead.TryDequeue(
                                       out var tcs))
                            {
                                tcs.TrySetResult(true);
                            }
                        }
                    }
                    return true;
                }
            }
        }

        public override bool TryWrite(TIn item)
        {
            lock (_parent._lockObj)
            {
                if (_parent._errorException == null)
                {
                    ChannelUtils.BufferedItemHolder<TBatch>? current =
                        _parent._item;
                    var cost = _parent._cost(item);
                    var mod = _parent._maxBuffer.GetValueOrDefault(4) - _parent._q.Count;
                    if (current == null)
                    {
                        var seeded = _parent._seed(item);
                        if (cost >= _parent._maxSize && mod>0)
                        {
                            _parent._q.Enqueue(seeded);
                        }
                        else
                        {
                            _parent._currentSize = cost;
                            _parent._item =
                                new ChannelUtils.BufferedItemHolder<TBatch>(seeded);
                        }
                        _parent.WakeUpReaders();
                        return true;
                    }
                    else
                    {
                        //We have a current item.
                        if (_parent._currentSize + cost > _parent._maxSize)
                        {
                            if (mod>0)
                            {
                                var pushItem = current.Item;
                                _parent._q.Enqueue(pushItem);
                                if (mod > 1)
                                {
                                    _parent._q.Enqueue(_parent._seed(item));
                                    _parent._currentSize = 0;
                                    _parent._item = default;
                                }
                                else
                                {
                                    _parent._item = new ChannelUtils.BufferedItemHolder<TBatch>(_parent._seed(item));
                                    _parent._currentSize = cost;   
                                }
                                
                                _parent.WakeUpReaders();
                                return true;
                            }
                            
                            return false;
                        }
                        else
                        {
                            _parent._currentSize = _parent._currentSize + cost;
                            _parent._item =
                                new ChannelUtils.BufferedItemHolder<TBatch>(
                                    _parent._add(item, current.Item));
                            _parent.WakeUpReaders();
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        public override ValueTask<bool> WaitToWriteAsync(
            CancellationToken cancellationToken = new CancellationToken())
        {
            if (_parent._q.IsEmpty == true || _parent._item!=null)
            {
                return new ValueTask<bool>(true);
            }

            lock (_parent._lockObj)
            {
                if (_parent._item != null)
                {
                    return new ValueTask<bool>(true);
                }
                if (_parent._q.IsEmpty == true)
                {
                    return new ValueTask<bool>(true);
                }
                if (_parent._errorException != null)
                {
                    return _parent._errorException ==
                           ChannelUtils.DoneWritingSentinel
                        ? new ValueTask<bool>(false)
                        : ValueTask.FromException<bool>(_parent
                            ._errorException);
                }
                else
                {
                    if (cancellationToken == default)
                    {
                        WaitingWriteOrReadItem.RentOrGet(_writerPool,
                            out var waiter2);
                       return waiter2.AsToken();
                    }
                    else
                    {
                        var waiter = new TaskCompletionSource<bool>(
                            TaskCreationOptions.RunContinuationsAsynchronously);
                        _parent._waitingForWaitToWrite.Enqueue(waiter);
                        cancellationToken.UnsafeRegister((obj, ct) =>
                        {
                            var innerTcs = (WaitingWriteOrReadItem)obj!;
                            innerTcs.SetCancelled(ct);
                        }, waiter);
                        return new ValueTask<bool>(waiter.Task);    
                    }
                    
                }
            }
        }
    }

    private void WakeUpReaders()
    {
        while (_waitingForWaitToRead.TryDequeue(out var wake))
        {
            wake.TrySetResult(true);
        }
    }

    private void WakeUpWriters()
    {
        while (_waitingForWaitToWrite.TryDequeue(out var wake))
        {
            wake.TrySetResult(true);
        }
    }

    protected class BatchingChannelReader : ChannelReader<TBatch>
    {
        private readonly BatchingChannel<TIn, TBatch> _parent;

        public BatchingChannelReader(BatchingChannel<TIn, TBatch> parent)
        {
            this._parent = parent;
        }
        //public BatchingChannelReader(ChannelReader<TIn> reader,
        //    int maxSize,
        //    Func<TIn, int> cost,
        //    Func<TIn, TBatch> seed,
        //    Func<TIn, TBatch, TBatch> add,
        //    int maxBuffer = 1,
        //    bool completeWriterOnReadComplete = true)
        //{
        //this._reader = reader;
        //this._writer = writer;
        //this._maxSize = maxSize;
        //this._cost = cost;
        //this._seed = seed;
        //this._add = add;
        //this._maxBuffer = maxBuffer;
        //this._completeWriterOnReadComplete = completeWriterOnReadComplete;
        //
        //this._buffer = new Queue<TBatch>(maxBuffer);
        //this._prepend = new List<TBatch>(1);
        //this._writeState = 0;
        //this._readState = 0;
        //}

        public override bool CanPeek => true;
        public override bool TryPeek(out TBatch item)
        {
            if (_parent._q.TryPeek(out item))
            {
                return true;
            }
            else
            {
                var c = _parent._item;
                if (c != null)
                {
                    item = c.Item;
                    return true;
                }

                return false;
            }
        }

        public override bool TryRead(out TBatch item)
        {
            if (_parent._q.TryDequeue(out item))
            {
                _parent.WakeUpWriters();
                return true;
            }
            else
            {
                lock (_parent._lockObj)
                {
                    if (_parent._q.TryDequeue(out item))
                    {
                        _parent.WakeUpWriters();
                        return true;
                    }

                    if (_parent._item != null)
                    {
                        item = _parent._item.Item;
                        _parent._item = null;
                        _parent._currentSize = 0;
                        _parent.WakeUpWriters();
                        return true;
                    }
                }
            }

            return false;
        }

        public override ValueTask<bool> WaitToReadAsync(
            CancellationToken cancellationToken = new CancellationToken())
        {
            if (_parent._q.TryPeek(out _))
            {
                return new ValueTask<bool>(true);
            }

            lock (_parent._lockObj)
            {
                if (_parent._q.TryPeek(out _))
                {
                    return new ValueTask<bool>(true);
                }

                if (_parent._item != null)
                {
                    return new ValueTask<bool>(true);
                }

                if (_parent._errorException != null)
                {
                    if (_parent._errorException ==
                        ChannelUtils.DoneWritingSentinel)
                    {
                        return new ValueTask<bool>(false);
                    }
                    else
                    {
                        return ValueTask.FromException<bool>(
                            _parent._errorException);
                    }
                }
                else
                {
                    if (cancellationToken == default)
                    {
                        WaitingWriteOrReadItem.RentOrGet(_writerPool,
                            out var waiter2);

                        return waiter2.AsToken();

                    }
                    else
                    {
                        var tcs = new TaskCompletionSource<bool>(
                            TaskCreationOptions
                                .RunContinuationsAsynchronously);
                        _parent._waitingForWaitToRead.Enqueue(tcs);
                        cancellationToken.UnsafeRegister((obj, token) =>
                        {
                            var newTcs = (TaskCompletionSource<bool>)obj!;
                            newTcs.TrySetCanceled(token);
                        }, tcs);
                        return new ValueTask<bool>(tcs.Task);
                    }
                }
            }
        }
    }
}