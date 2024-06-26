using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace GlutenFree.Synchronizaton
{
    public class FailureRecurringLockProvider
    {
        private readonly Channel<WaitOrCompletionLock> _provider =
            Channel.CreateUnbounded<WaitOrCompletionLock>();

        private readonly object _resetLock = new();
        public bool TryGetCurrentLock(out WaitOrCompletionLock? completionLock)
        {
            return _provider.Reader.TryPeek(out completionLock);
        }

        public bool TrySetErrorOnCurrentAndReset(
            Exception exception)
        {
            if (Monitor.TryEnter(_resetLock))
            {
                if (_provider.Reader.TryPeek(out WaitOrCompletionLock? item))
                {
                    item.MarkHardFailure(exception);
                    item.
                }
            }
            else
            {
                return false;
            }
        }
    }
    public class WaitOrCompletionLock
    {
        private readonly Channel<int> lockChannel = Channel.CreateBounded<int>(1);
	
        /// <summary>
        /// If -true-, you can continue.
        /// <para/>
        /// If -false-, you are done.
        /// <para/>
        /// If Throws, a hard-fault was signalled.
        /// <para/>
        /// If you are using a retry loop on this, try to re-use a single waiter
        /// Instead of calling it repeatedly. :)
        /// </summary>
        public async Task<bool> WaitForNext(CancellationToken token = default)
        {
            return await lockChannel.Reader.WaitToReadAsync(token);
        }
	
        /// <summary>
        /// Tries to set the waiter as no longer waiting.
        /// </summary>
        /// <returns>If -false-, the waiter was already set to not-waiting</returns>
        public bool TrySetNotWaiting()
        {
            return lockChannel.Writer.TryWrite(0);
        }
	
        /// <summary>
        /// Tries to set the waiter as waiting.
        /// </summary>
        /// <returns>If -false-, the waiter was already set as waiting</returns>
        public bool TrySetWaiting()
        {
            return lockChannel.Reader.TryRead(out _);
        }
	
        /// <summary>
        /// Marks the waiter as -completed-, i.e. no further work should be done.
        /// </summary>
        /// <returns></returns>
        public bool MarkCompleted()
        {
            var didComplete = lockChannel.Writer.TryComplete();
            lockChannel.Reader.TryRead(out _);
	    return didComplete;
        }
	
        /// <summary>
        /// Marks the waiter in a hard failure state.
        /// </summary>
        public bool MarkHardFailure(Exception ex)
        {
            var didComplete = lockChannel.Writer.TryComplete(ex);
	    lockChannel.Reader.TryRead(out _);
            return didComplete;
        }
    }
}