using System.Collections.Concurrent;
using System.Threading.Channels;

namespace JFA.Channels;

public class ChannelFilter<T> : ChannelReader<T>
{
    [Flags]
    public enum State
    {
        NoReadersOrWaiters = 0,
        Readers = 1,
        Waiters = 2,
        ReadersAndWaiters=3
    }
    private class Buffer{}
    private sealed class NoBufferedItem : Buffer
    {
        public static readonly NoBufferedItem Instance = new();
    }

    private sealed class BufferedItem : Buffer
    {
        private readonly T Item;

        public BufferedItem(T item)
        {
            this.Item = item;
        }
    }

    private static readonly Buffer NoBuffer = new NoBufferedItem();
    private readonly Predicate<T> pred;
    private readonly ChannelReader<T> reader;
    public override bool CanPeek => reader.CanPeek;
    private readonly object lockObj = new();
    private Exception? _parentClosed = null; 
    private readonly ConcurrentQueue<T> _bufferedItems =
        new ConcurrentQueue<T>();

    private readonly TaskCompletionSource _completion =
        new TaskCompletionSource(TaskCreationOptions
            .RunContinuationsAsynchronously);
    private readonly Task _completionTask;
    
    public override bool TryPeek(out T item)
    {
        if (CanPeek == false)
        {
            item = default!;
            return false;
        }
        else if (_bufferedItems.TryPeek(out item) 
                 || (reader.TryPeek(out item) && pred(item)))
        {
            return true;
        }
        else
        {
            return false;
        }
    }

    public ChannelFilter(ChannelReader<T> reader, Predicate<T> predicate)
    {
        this.pred = predicate;
        this.reader = reader;
        this._completionTask = reader.Completion.ContinueWith(async t =>
        {
            await _completion.Task;
            
            if (t.IsFaulted)
            {
                await t;
            }
        });
    }
    public override bool TryRead(out T item)
    {
        
        if (_bufferedItems.TryDequeue(out item))
        {
            return true;
        }
        else
        {
            while (true)
            {
                if (_bufferedItems.TryDequeue(out item))
                {
                    return true;
                }
                else if (reader.TryRead(out item) == false)
                {
                    return false;
                }
                else
                {
                    if (pred(item))
                    {
                        return true;
                    }   
                }
            }   
        }

        item = default!;
        return false;
    }

    public override async ValueTask<bool> WaitToReadAsync(
        CancellationToken cancellationToken = default)
    {
        if (_bufferedItems.IsEmpty == false)
        {
            return true;
        }
        else if (_parentClosed != null)
        {
            if (_parentClosed != ChannelUtils.DoneWritingSentinel)
            {
                return false;
            }
        }

        bool itemFound = false;
        
        while (true)
        {
            var hasNext = await reader.WaitToReadAsync(cancellationToken);
            lock (lockObj)
            {
                if (_bufferedItems.IsEmpty==false)
                {
                    return true;
                }

                if (hasNext == false)
                {
                    if (_bufferedItems.IsEmpty)
                    {
                        _completion.TrySetResult();   
                    }

                    _parentClosed = ChannelUtils.DoneWritingSentinel;
                    return false;
                }
                
                while (reader.TryRead(out var maybe))
                {
                    if (pred(maybe))
                    {
                        itemFound = true;
                        _bufferedItems.Enqueue(maybe);
                        return true;
                    }
                }
            }
        }
    }
}