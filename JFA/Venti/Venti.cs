using System.Buffers;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Faster.Map;
using MessagePack;
using MessagePack.Formatters;

namespace JFA.Venti;

public class Venti
{
    
}

public readonly struct VentiKey
{
    public readonly string Key;

    public VentiKey(string key)
    {
        Key = key;
    }
}

public class VentiSubHandler
{
    
}

public readonly struct VentiInClock
{
    public readonly VentiKey Key; 
    public readonly long Seq;
}

public class VentiOutClock
{
    
}

public abstract class VentiSubEventHandler
{
    public abstract ValueTask OnMessageReady(VentiInClock clock, VentiSubscription sub);

    public abstract ValueTask OnClose(VentiInClock clock, VentiSubscription subscription);

    public abstract ValueTask OnError(VentiInClock clock, VentiSubscription subscription);
}

public class VentiMainHandler
{
    
}


public abstract class VentiSubscription
{
    public VentiKey Key { get; }
    protected VentiSubHandler _handler { get; }
    protected VentiIncarnation _parent { get; }

    public async ValueTask Close()
    {
        
    }
}

public abstract class VentiSubReader
{
    public abstract ValueTask<bool> IsReady();
    public abstract bool TryReadAndIncrement<T>(out T message);
    public abstract bool TryPeek<T>(out T message);
}

public abstract class VentiWriter
{
    
}

public class VentiWrite
{
    public VentiKey Key { get; set; }
    public long Sequence { get; set; }
    public IMemoryOwner<byte> Bytes { get; set; }
}

public abstract class VentiSerializer
{
    public abstract IMemoryOwner<byte> Serialize<T>(T message);
    public abstract T Deserialize<T>(IMemoryOwner<byte> message);
    
}

public abstract class FastDispatcherThread<TWorkQueue, TWorkItem> where TWorkQueue : IFastDispatcherWorkQueue<TWorkItem>
{
    private readonly TWorkQueue _workQueue;
    private readonly Thread _thread;

    public FastDispatcherThread(TWorkQueue workQueue)
    {
        _workQueue = workQueue;
        _thread = new Thread((start) =>
        {
            if (start is FastDispatcherThread<TWorkQueue, TWorkItem> fdt)
            {
                if (fdt.TryGetWorkItem(out var item))
                {
                    ProcessWorkItem(item);
                }
            }
        });
    }

    protected abstract void ProcessWorkItem(TWorkItem item);
    private bool TryGetWorkItem(out TWorkItem item)
    {
        if (_workQueue.TryGetWorkItem(out item))
        {
            return true;
        }

        return false;
    }

    public void Run()
    {
        _thread.Start();
    }
}

public interface IFastDispatcherWorkQueue<TWorkItem>
{
    bool TryGetWorkItem(out TWorkItem item);
    void Add(TWorkItem workItem);
}

[Flags]
public enum ValTypeEnum
{
    Uninitialized = 0,
    IsNullable = (short)1,
    IsNull = (short)3,//just to make sure we are nullable
    IsInt = (short)4,
    IsLong=(short)8,
    IsBool=(short)16,
    IsChar=(short)32,
    IsFloat=(short)64,
    IsDouble=(short)128,
    IsByte=(short)256,
    IsShort = (short)512,
    IsHalf = (short)1024,
    IsNullableInt = IsInt | IsNullable,
    IsNullableLong = IsLong | IsNullable,
    IsNullableBool = IsBool | IsNullable
}

[StructLayout(LayoutKind.Explicit)]
public readonly struct VentiHeaderVal32
{
    [FieldOffset(0)] 
    public readonly short ValType;
    [FieldOffset(2)] public readonly Int16 RawData;
    private VentiHeaderVal32(ValTypeEnum nullableValType)
    {
        ValType = (short)nullableValType;
    }
    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    public static VentiHeaderVal32 From(bool? boolVal)
    {
        if (boolVal.HasValue)
        {
            return new VentiHeaderVal32(ValTypeEnum.IsNullableBool,ToInt16(boolVal.Value));
        }
        else
        {
            return new VentiHeaderVal32(ValTypeEnum.IsNullableBool |
                                        ValTypeEnum.IsNull);
        }
    }

    private static Int16 ToInt16<T>(T val) where T : struct
    {
        return Unsafe.As<T, Int16>(ref val);
    }

    public static VentiHeaderVal32 From(bool boolVal)
    {
        return new VentiHeaderVal32(ValTypeEnum.IsBool,
            ToInt16(boolVal));
    }

    private VentiHeaderVal32(ValTypeEnum typ, Int16 rawData)
    {
        ValType = (short)typ;
        RawData = rawData;
    }
}

[StructLayout(LayoutKind.Explicit)]
public readonly struct VentiHeaderVal64
{
    [FieldOffset(0)]
    public readonly short ValType;

    [FieldOffset(4)] public readonly int RawData;

    private VentiHeaderVal64(ValTypeEnum typeEnum)
    {
        ValType = (short)typeEnum;
    }

    public static VentiHeaderVal64 From(int? value)
    {
        return FromImpl(value);
    }

    private static VentiHeaderVal64 FromImpl<T>(T value,
        bool shouldBeNullable = false) where T: struct
    {
        ValTypeEnum toUse = shouldBeNullable
            ? ValTypeEnum.IsNullable
            : ValTypeEnum.Uninitialized;
        if (typeof(T) == typeof(int))
        {
            toUse = toUse | ValTypeEnum.IsInt;
        }

        if (typeof(T) == typeof(float))
        {
            toUse = toUse | ValTypeEnum.IsFloat;
        }

        return new VentiHeaderVal64(toUse, Unsafe.As<T, int>(ref value));
    }
    private static VentiHeaderVal64 FromImpl<T>(T? value) where T : struct
    {
        if (value.HasValue)
        {
            return FromImpl(value.Value, true);
        }
        if (typeof(T) == typeof(bool))
        {
            return new VentiHeaderVal64(ValTypeEnum.IsNullableBool |
                                        ValTypeEnum.IsNull);
        }
        else if (typeof(T) == typeof(int))
        {
                return new VentiHeaderVal64(ValTypeEnum.IsNullableInt |
                                            ValTypeEnum.IsNull);
        }
        else if (typeof(T) == typeof(float))
        {
                return new VentiHeaderVal64(ValTypeEnum.IsFloat |
                                            ValTypeEnum.IsNullable |
                                            ValTypeEnum.IsNull);
        }
        else
        {
            return default;
        }
    }
    private static Int32 ToInt32<T>(T val) where T : struct
    {
        return Unsafe.As<T, Int32>(ref val);
    }
    private VentiHeaderVal64(ValTypeEnum typeEnum, int rawData)
    {
        ValType = (short)typeEnum;
        RawData = rawData;
    }
}
[StructLayout(LayoutKind.Explicit)]
public readonly struct VentiHeaderVal
{
    [FieldOffset(0)]
    public readonly short ValType;
    [FieldOffset(4)]
    public readonly int intKey;
    [FieldOffset(4)]
    public readonly long longKey;
    [FieldOffset(4)]
    public readonly bool boolKey;
    public ValTypeEnum Type()
    {
        return (ValTypeEnum)ValType;
    }
}

[StructLayout(LayoutKind.Explicit)]
public readonly struct VentiHeaderContainer
{
    [FieldOffset(0)] public readonly byte type;
    [FieldOffset(1)] private readonly VentiHeaderVal32 _headerVal32;
    [FieldOffset(1)] private readonly VentiHeaderVal64 _headerVal64;
}

public readonly struct VentiHeaderString
{
    public readonly string String;
}
public readonly struct VentiMessagePayload<T>
{
    public readonly long Seq;
    public readonly VentiKey Key;
    public readonly T Message;
    public readonly DenseMapSIMD<string, VentiHeaderVal> Headers;

    public VentiMessagePayload(VentiKey key, long seq, T message, IEnumerable<(string, VentiHeaderVal)> headers)
    {
        Seq = seq;
        Key = key;
        Message = message;
        var map = new DenseMapSIMD<string, VentiHeaderVal>();
        foreach (var h in headers)
        {
            map.Emplace(h.Item1, h.Item2);
        }
    }
}


public class MessagePackVentiPayloadResolver : IFormatterResolver
{
    private static DenseMapSIMD<Type, IMessagePackFormatter> formatters = new();
    public IMessagePackFormatter<T>? GetFormatter<T>()
    {
        if (typeof(T).IsClass == false && typeof(T).IsConstructedGenericType &&
            typeof(T).GetGenericTypeDefinition() ==
            typeof(VentiMessagePayload<>))
        {
            var innerType = typeof(T).GetGenericArguments()[0];
            return (IMessagePackFormatter<T>)
                typeof(MessagePackVentiMessageFormatter<>)
                    .MakeGenericType(innerType)
                    .GetConstructor(Array.Empty<Type>())!.Invoke(null);
        }

        return null;
    }
    
}

public static class FormatterCache<T>
{
    public static readonly MessagePackVentiMessageFormatter<T> _ventiFormatter =
        new();
}

public class
    MessagePackVentiMessageFormatter<T> : IMessagePackFormatter<
        VentiMessagePayload<T>>
{
    public void Serialize(ref MessagePackWriter writer, VentiMessagePayload<T> value,
        MessagePackSerializerOptions options)
    {
        writer.WriteArrayHeader(4);
        writer.Write(value.Key.Key);
        writer.Write(value.Seq);
        options.Resolver.GetFormatter<T>()!.Serialize(ref writer,value.Message,options);
        writer.WriteMapHeader(value.Headers.Count);
        foreach (var val in value.Headers.Entries)
        {
            writer.Write(val.Key);
            switch (val.Value.Type())
            {
                case ValTypeEnum.IsBool:
                    writer.Write(val.Value.boolKey);
                    break;
                case ValTypeEnum.IsInt:
                    writer.WriteInt32(val.Value.intKey);
                    break;
                case ValTypeEnum.IsLong:
                    writer.WriteInt64(val.Value.longKey);
                    break;
                default:
                    throw new NotImplementedException();
            }
        }
    }

    public VentiMessagePayload<T> Deserialize(ref MessagePackReader reader,
        MessagePackSerializerOptions options)
    {
        var h = reader.ReadArrayHeader();
        var key = reader.ReadString();
        var seq = reader.ReadInt64();
        var msg = options.Resolver.GetFormatter<T>()!
            .Deserialize(ref reader, options);
        var headers = new (string, VentiHeaderVal)[reader.ReadMapHeader()];
        
        return new VentiMessagePayload<T>(new VentiKey(key!), seq, msg, headers);
    }
}

public abstract class VentiIncarnation
{
    private ConcurrentDictionary<string, VentiSubscription> _subs = new();
    private ConcurrentDictionary<string, VentiInClock> _ins;
    private ConcurrentDictionary<string, VentiOutClock> _outs;
    private ConcurrentDictionary<string, VentiSubEventHandler> _handlers;
    private VentiMainHandler _mainHandler;
    private readonly string _key;
    public abstract ValueTask<bool> Subscribe(string key);
    // Interface, class, or Default struct of funcs for above's handlers?
    //
}

public readonly struct VentiContext
{
    private readonly VentiIncarnation _incarnation;
    //
}