using System.Buffers;
using JFA.Channels.Pool;

namespace JFA.Composition;

public class CompositonEx
{
    
}

public class ReleasableArray<T> : IDisposable
{
    private bool isDisposed;
    private readonly ArrayPool<T> _holdingPool;
    private readonly T[] array;

    public T this[int i] => array[i];

    public void Dispose()
    {
        if (isDisposed == false)
        {
            if (Interlocked.(ref isDisposed, true, false))
            if (_holdingPool != null)
            {
                _holdingPool.Return(array);
            }
            GC.SuppressFinalize(this);
        }
    }

    ~ReleasableArray()
    {
      Dispose();  
    }
}