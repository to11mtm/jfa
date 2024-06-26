using System.Buffers;
using LinqToDB.Data;

namespace BigMessageSplitter;

public class TaskSerializer
{
    
}

public class RemoteFuture<TResult>
{
    public readonly Ulid Id;

    public readonly RemoteFutureResult<TResult> Result =
        new RemoteFutureResult<TResult>();
}

public class RemoteFutureResult<TResult>
{
    
}


public class SplitSender
{
    private readonly ISplitSenderRepo _repo;

    public async ValueTask SaveAndDo<TState, T>(TState state, T toDo,
        Func<TState, SplitReq<T>, ValueTask> whenSaved)
    {
        
    }
}

public class SplitReq<T>
{
    public T Value { get; }
    public Ulid RequestId { get; }

    public SplitReq(T value, Ulid requestId)
    {
        Value = value;
        RequestId = requestId;
    }
}

public interface ISplitSenderRepo
{
    ValueTask SaveEntry<T>(long? equivHash, string? handlingAppHeader, Ulid id, T value);
}

public interface IDataConnectionFactory
{
    DataConnection GetConnection();
}

public interface ISplitSerializer
{
    int SerializerId { get; }

    (IMemoryOwner<byte> header, IMemoryOwner<byte> payload)
        GetReconstructablePayloadAndManifest<T>(T entry);
}

public readonly struct RPAM
{
    public readonly IMemoryOwner<byte> Header;
    public readonly IMemoryOwner<byte> Payload;

    public RPAM(IMemoryOwner<byte> header, IMemoryOwner<byte> payload)
    {
        Header = header;
        Payload = payload;
    }
}

public enum ProcState
{
    New,
    Processing,
    Processed,
    Cancelled,
    Errored
}

public abstract class FuncHolder<TState,T>
{
    public abstract Func<TState, SplitReq<T>, ValueTask> WhenSaved { get; }
    public abstract Func<TState, SplitReq<T>, ProcState, ValueTask> OnRecovery
    {
        get;
    } 
    public abstract Func<TState, SplitReq<T>, ValueTask> OnFailure { get; }
}
public class SplitSenderSqlRepo : ISplitSenderRepo
{
    private readonly IDataConnectionFactory _factory;
    private readonly Dictionary<string, Task<object,>
    public async ValueTask SaveEntry<T>(long? equivHash, string? handlingAppHeader, Ulid id,
        T value)
    {
    }
}