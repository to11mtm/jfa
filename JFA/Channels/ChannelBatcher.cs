using System.Threading.Channels;

namespace JFA.Channels;

public class SingleReaderBatchingChannelReader<TIn,TOut> : ChannelReader<TOut>
{
    private readonly ChannelReader<TIn> _input;
    private readonly int _maxCost;
    private readonly Func<TIn, int> _cost;
    private readonly Func<TIn, TOut> _seed;
    private readonly Func<TIn, TOut, TOut> _agg;
    private int currCost = 0;
    private int currItems = 0;
    private TOut _item = default;
    private bool hasItem = false;
    public SingleReaderBatchingChannelReader(ChannelReader<TIn> batch,
        int maxCost, Func<TIn, int> cost, Func<TIn, TOut> seed,
        Func<TIn, TOut, TOut> agg)
    {
        _input = batch;
        _maxCost = maxCost;
        _cost = cost;
        _seed = seed;
        _agg = agg;
    }
    public override bool TryRead(out TOut item)
    {
        if (hasItem)
        {
            item = this._item;
            hasItem = false;
        }
        else
        {
            if (_input.TryRead(out var newItem))
            {
                currCost = _cost(newItem);
                item = _seed(newItem);
                hasItem = true;
            }
            else
            {
                item = default!;
                return false;
            }
        }

        var thisCost = currCost;
        currCost = 0;
        while (thisCost < _maxCost)
        {
            if (_input.TryRead(out var newItem))
            {
                var nextCost = this._cost(newItem);
                if (thisCost + nextCost <= _maxCost)
                {
                    item = _agg(newItem, item);
                    thisCost = nextCost;
                }
                else
                {
                    _item = _seed(newItem);
                    currCost = nextCost;
                    thisCost = _maxCost + 1;
                }
            }
        }

        return true;
    }

    public override async ValueTask<bool> WaitToReadAsync(
        CancellationToken cancellationToken = new CancellationToken())
    {
        throw new NotImplementedException();
    }
}
public class ChannelBatcher<TIn,TBatch>
{
    private ChannelReader<TIn> _reader;
    private ChannelWriter<TBatch> _writer;
    private int _maxSize;
    private Func<TIn, int> _cost;
    private Func<TIn, TBatch> _seed;
    private Func<TIn, TBatch, TBatch> _add;
    private int _maxBuffer;
    private bool _completeWriterOnReadComplete;
    private readonly Queue<TBatch> _buffer;
    private readonly List<TBatch> _prepend;
    private Task<bool> _readTask;
    private Task<bool> _writeTask;
    protected int _writeState;
    private int _readState;
    private TBatch _batch = default;
    private int _batchSize = 0;
    private int _batchItems = 0;

    private TaskCompletionSource _completionSource =
        new TaskCompletionSource(TaskCreationOptions.DenyChildAttach);
    public ChannelBatcher(ChannelReader<TIn> reader,
        ChannelWriter<TBatch> writer,
        int maxSize,
        Func<TIn, int> cost,
        Func<TIn, TBatch> seed,
        Func<TIn, TBatch, TBatch> add,
        int maxBuffer = 1,
        bool completeWriterOnReadComplete = true)
    {
        this._reader = reader;
        this._writer = writer;
        this._maxSize = maxSize;
        this._cost = cost;
        this._seed = seed;
        this._add = add;
        this._maxBuffer = maxBuffer;
        this._completeWriterOnReadComplete = completeWriterOnReadComplete;
        
        this._buffer = new Queue<TBatch>(maxBuffer);
        this._prepend = new List<TBatch>(1);
        this._writeState = 0;
        this._readState = 0;
    }

    public async Task Run()
    {

        this._readTask = _reader.WaitToReadAsync().AsTask();
        this._writeTask = _writer.WaitToWriteAsync().AsTask();
        while (true)
        {
            if (_readState > 0 && _writeState == 0)
            {
                await _writeTask;
            }
            else if (_writeState == 0 && _readState == 0)
            {
                await Task.WhenAny(_readTask, _writeTask).ConfigureAwait(false);

                if (_writeTask.IsCompletedSuccessfully)
                {
                    _writeState = _writeTask.Result ? 1 : 2;
                }

                if (_readTask.IsCompletedSuccessfully)
                {
                    _readState = _readTask.Result ? 1 : 2;
                }
                else if (_readTask.IsFaulted)
                {
                    _readState = 3;
                }
            }

            while (_readState == 1)
            {
                if (TryDoReads()>0) break;
            }

            if (_writeState == 1)
            {
                TryFlushFlushNormally();
            }
            else if (_writeState > 1)
            {
                //TODO: Check Read state appropriately.
            }

            if (_readState > 1)
            {
                await FlushWriteAfterReaderCompleteImpl(_batchSize,_batch);
                return;
            }
        }
    }

    protected StatusEnum GetState()
    {
        var retVal = StatusEnum.NoState;
        if (_readTask?.IsCompletedSuccessfully == true)
        {
            retVal = retVal | StatusEnum.ReadersReady;
        }
        else
        {
            retVal = StatusEnum.ReadersWaiting;
        }
        if (_writeTask?.IsCompletedSuccessfully == true)
        {
            retVal = retVal | StatusEnum.WritersReady;
        }
        else
        {
            retVal = retVal | StatusEnum.WritersWaiting;
        }

        return retVal;
    }
    protected int TryDoReads()
    {
        if (_buffer.Count >= _maxBuffer)
        {
            return 2;
        }
        if (_reader.TryRead(out var item) == false)
        {
            _readState = 0;
            _readTask = _reader.WaitToReadAsync().AsTask();
            return 1;
        }

        var bufferedItemSize = _cost(item);
        if (_batchSize == 0)
        {
            _batch = _add(item, _seed(item));
            _batchSize = bufferedItemSize;
            _batchItems = _batchItems + 1;
        }
        else if (bufferedItemSize + _batchSize <= _maxSize)
        {
            _batch = _add(item, _batch!);
            _batchSize = _batchSize + bufferedItemSize;
            _batchItems = _batchItems + 1;
        }
        else if (_batchSize > 0)
        {
            _buffer.Enqueue(_batch!);
            _batch = _seed(item);
            _batchSize = bufferedItemSize;
            _batchItems = 1;
            bufferedItemSize = 0;
        }
        else if (bufferedItemSize > _maxSize)
        {
            _buffer.Enqueue(_seed(item));
        }

        return 0;
    }

    protected int TryFlushFlushNormally()
    {
        int written = 0;
        while (_prepend.Count > 0)
        {
            if (_writer.TryWrite(_prepend[0]))
            {
                _prepend.RemoveAt(0);
                written = written+1;
            }
            else
            {
                _writeState = 0;
            }
        }

        if (written == 0)
        {
            return written;
        }
        while (_buffer.TryDequeue(out var write))
        {
            if (_writer.TryWrite(write) == false)
            {
                _prepend.Add(write);
                _writeState = 0;
                break;
            }
            else
            {
                written = written+1;
            }
        }

        if (_writeState == 0)
        {
            _writeTask = _writer.WaitToWriteAsync().AsTask();
        }

        return written;
    }

    private async Task FlushWriteAfterReaderCompleteImpl(
        int batchSize, TBatch batch)
    {
        while (await _writer.WaitToWriteAsync())
        {
            if (_prepend.Count > 0)
            {
                if (_writer.TryWrite(_prepend[0]))
                {
                    _prepend.RemoveAt(0);
                }
            }
            else if (_buffer.Count > 0)
            {
                if (_buffer.TryDequeue(out var write))
                {
                    if (_writer.TryWrite(write) == false)
                    {
                        _prepend.Add(write);
                    }
                }
            }
            else if (_batchItems > 0)
            {
                if (_writer.TryWrite(batch!))
                {
                    _batchItems = 0;
                    batch = default!;
                }
            }
            else
            {
                if (this._completeWriterOnReadComplete)
                {
                    if (_readTask.IsFaulted)
                    {
                        try
                        {
                            await _readTask;
                        }
                        catch (Exception e)
                        {
                            _writer.TryComplete(e);
                        }
                    }
                    else
                    {
                        _writer.TryComplete();   
                    }   
                }

                break;
            }
        }

        return;
    }

    
}