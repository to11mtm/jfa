using System.Collections.Concurrent;
using System.Threading.Channels;

namespace JFA.Scheduler;

public class TrashScheduler
{
    
    private readonly ConcurrentQueue<ValueTask> _workQueue = new();

    private static readonly UnboundedChannelOptions Opts =
        new UnboundedChannelOptions()
        {
            AllowSynchronousContinuations = false,
            SingleReader = true,
            SingleWriter = false
        };
    private readonly Channel<object> _unboundedMpSc = Channel.CreateUnbounded<object>(Opts);

    public void AddWorkItem(Func<ValueTask> workItem)
    {
        
    }

    public void AddWorkItem<T>(Func<ValueTask<T>> workItem)
    {
        
    }

    private class PooledWorkItem<TDel,TArgs> : IThreadPoolWorkItem where TDel: Delegate
    {
        public TrashScheduler Parent;
        public TDel Delegate;
        public Action<TDel, TArgs> RunWorkItem;
        public TArgs Args;
        public void Execute()
        {
            RunWorkItem(Delegate, Args);
        }
    }

    private interface IWorkItemWithCompletionCall : IThreadPoolWorkItem
    {
        void Complete();
    }
}
