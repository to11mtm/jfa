using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using System.Text.Json;
using LanguageExt;
using MessagePack;
using MessagePack.Formatters;
using Microsoft.Extensions.Primitives;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Client.KeyValueStore;

namespace NATSWrappers;

//via https://stackoverflow.com/a/75795756/2937845


public enum SimpleExtDataType
{
    Nil,
    Bool,
    Byte,
    Int,
    Double,
    Long
}

public readonly record struct SimpleExtData
{
    public readonly SimpleExtDataType Type;
    internal readonly long RawData;

    public SimpleExtData(int val)
    {
        Type = SimpleExtDataType.Int;
        RawData = val;
    }

    public SimpleExtData(bool val)
    {
        Type = SimpleExtDataType.Bool;
        RawData = 1;
    }

    public SimpleExtData(double val)
    {
        var conv = val;
        Type = SimpleExtDataType.Bool;
        RawData = Unsafe.As<double,long>(ref conv);
    }

    public SimpleExtData(long val)
    {
        Type = SimpleExtDataType.Long;
        RawData = val;
    }

    public SimpleExtData()
    {
        Type = SimpleExtDataType.Nil;
        RawData = default;
    }

    public SimpleExtData(byte val)
    {
        Type = SimpleExtDataType.Byte;
        RawData = val;
    }
}
    

public enum ComplexData
{
    String,
    Typeless
}

public class AdaptableCausalityWrappedValue<T>
{
    public T? Value { get; init; }
    public EventParentContext? Parent { get; init; }
    public EventCause? Cause { get; init; }
    public SlimOption<T> PreviousValue { get; init; }
    //public HashMap<string, >
}

public class
    CausalityWrappedValueFormatter<T> : IMessagePackFormatter<CausalityWrappedValue<T>> where T : class
{
    private readonly IMessagePackFormatter<T> _formatter;

    public CausalityWrappedValueFormatter(IMessagePackFormatter<T> formatter)
    {
        _formatter = formatter;
    }
    public void Serialize(ref MessagePackWriter writer,
        CausalityWrappedValue<T> value,
        MessagePackSerializerOptions options)
    {
        writer.WriteArrayHeader(4);
        options.Resolver.GetFormatter<EventCause>()!
            .Serialize(ref writer, value.Cause, options);
        if (value.Parent == null)
        {
            writer.WriteNil();
        }
        else
        {
            options.Resolver.GetFormatter<EventParentContext>()!
                .Serialize(ref writer, value.Parent, options);
        }

        options.Resolver.GetFormatter<T>()!
            .Serialize(ref writer, value.Value, options);
        options.Resolver.GetFormatter<SlimOption<T>>()!
            .Serialize(ref writer, value.PreviousValue, options);
    }

    CausalityWrappedValue<T> IMessagePackFormatter<CausalityWrappedValue<T>>.
        Deserialize(ref MessagePackReader reader,
            MessagePackSerializerOptions options)
    {
        throw new NotImplementedException();
    }

}
public class
    AbstractCausalityWrappedValueFormatter<TC,T> : IMessagePackFormatter<TC> where TC:CausalityWrappedValue<T>
{
    private readonly IMessagePackFormatter<T> _formatter;

    public AbstractCausalityWrappedValueFormatter(IMessagePackFormatter<T> formatter)
    {
        _formatter = formatter;
    }
    public void Serialize(ref MessagePackWriter writer,
        TC value,
        MessagePackSerializerOptions options)
    {
        writer.WriteArrayHeader(4);
        options.Resolver.GetFormatter<EventCause>()!
            .Serialize(ref writer, value.Cause, options);
        if (value.Parent == null)
        {
            writer.WriteNil();
        }
        else
        {
            options.Resolver.GetFormatter<EventParentContext>()!
                .Serialize(ref writer, value.Parent, options);
        }

        options.Resolver.GetFormatter<T>()!
            .Serialize(ref writer, value.Value, options);
        options.Resolver.GetFormatter<SlimOption<T>>()!
            .Serialize(ref writer, value.PreviousValue, options);
    }

    public TC Deserialize(ref MessagePackReader reader,
        MessagePackSerializerOptions options)
    {
        var size = reader.ReadArrayHeader();
        if (size == 4)
        {
            EventCause? cause = null;
            EventParentContext context = null;
            T? value = default;
            SlimOption<T>? prevVal = default;
            
                cause = options.Resolver.GetFormatter<EventCause>()!.Deserialize(
                    ref reader, options);
                if (reader.TryReadNil() == false)
                {
                    context =
                        options.Resolver.GetFormatter<EventParentContext>()!
                            .Deserialize(ref reader, options);
                }
            value = options.Resolver.GetFormatter<T>()!
                .Deserialize(ref reader, options);
            prevVal = options.Resolver.GetFormatter<SlimOption<T>>()!
                .Deserialize(ref reader, options);
        }

        return null!;
    }
}
public class WrappedOrUnwrappedValueFormatter<T> : IMessagePackFormatter<WrappedOrUnwrappedValue<T>> where T : class
{
    public void Serialize(ref MessagePackWriter writer, WrappedOrUnwrappedValue<T> value,
        MessagePackSerializerOptions options)
    {
        throw new NotImplementedException();
    }

    public WrappedOrUnwrappedValue<T> Deserialize(ref MessagePackReader reader,
        MessagePackSerializerOptions options)
    {
        throw new NotImplementedException();
    }
}
public class WrappedOrUnwrappedValue<T> where T : class
{
    private readonly object _wrappedEvent;

    public T? Value => (_wrappedEvent is CausalityWrappedValue<T> a)
        ? a.Value
        : _wrappedEvent as T;

    public CausalityWrappedValue<T>? WrappedValue => _wrappedEvent as CausalityWrappedValue<T>;

    public WrappedOrUnwrappedValue(T value)
    {
        _wrappedEvent = value;
    }

    public WrappedOrUnwrappedValue(CausalityWrappedValue<T> value)
    {
        _wrappedEvent = value;
    }
}
public class WrappedOrUnwrappedTypeFormatter<T>
{
    
}

public static class Exts
{
    public static async ValueTask<CreateResult<T>> DoCreate<T>(this INatsKVStore store, string key, T value, bool writeOnDeleted = false,
        INatsSerializer<T>? serializer = default, CancellationToken cancellationToken = default)
    {
        try
        {
            await store.UpdateAsync(key, value, revision: 0, serializer, cancellationToken);
            return new CreateResult<T>(CreateResultStatus.Created, value);
        }
        catch (NatsKVWrongLastRevisionException)
        {
        }

        // If that fails, try to update an existing entry which may have been deleted
        try
        {
            var cv = await store.GetEntryAsync<T>(key, cancellationToken: cancellationToken);
            return new CreateResult<T>(CreateResultStatus.AlreadyExisted, cv.Value!);
        }
        catch (NatsKVKeyDeletedException e)
        {
            if (writeOnDeleted)
            {
                await store.UpdateAsync(key, value, e.Revision, serializer,
                    cancellationToken);
                return new CreateResult<T>(CreateResultStatus.Recreated, value);
            }
            else
            {
                return new CreateResult<T>(CreateResultStatus.DeletedNoRecreate,
                    default);
            }
        }

        throw new Exception("Unexpected condition in Create");
    }
    
    public static string? GetFirstOrNull(this NatsHeaders? headers, string key)
    {
        if (headers != null && headers.TryGetValue(key, out var vals))
        {
            return  vals.FirstOrDefault(key);
        }
        else
        {
            return default;
        }
    }
}

    public record NatsJsParentStateWrapper
    {
        public NatsJsParentStateWrapper(ref NatsJSMsg<object> msg)
        {
            throw new NotImplementedException();
        }
    }

    public record EventParentContext
    {
        public string EventContextCreatedFrom { get; init; } 
        public ulong EventContextConsumerSequence { get; init; }

        public NatsJSSequencePair EventContextSequence { get; init; }

        public string EventContextConsumer { get; init; }

        public DateTimeOffset EventContextTimeStamp { get; init; }

        public string EventContextParentStream { get; init; }

        public ulong EventContextStreamSequence { get; init; }

        public string EventContextDomain { get; init; }

        public string ParentStream { get; init; }
        public string? EventContextWasResultOfStream { get; init; }
        public string? EventContextWasResultOfEventSeq { get; init; }

    }
    
    public record NatsWrappingEvent<T>
    {
        public T Value { get; init; }
        public EventParentContext? Parent { get; init; }
        public EventCause Cause { get; init; }
    }

    public record CausalityWrappedValue<T>
    {
        public T? Value { get; init; }
        public EventParentContext? Parent { get; init; }
        public EventCause? Cause { get; init; }
        public SlimOption<T> PreviousValue { get; set; }
    }

    public readonly struct SlimOption<T>
    {
        public readonly bool HasValue;
        public readonly T? Value;

        public SlimOption(T value)
        {
            HasValue = true;
            Value = value;
        }

        public SlimOption(bool hasValue, T value)
        {
            HasValue = hasValue;
            Value = value;
        }

        public SlimOption()
        {
            HasValue = false;
            Value = default!;
        }
    }

    public class HasValueSentinel : Exception
    {
        public static readonly HasValueSentinel Instance = new HasValueSentinel();
    }
    public readonly struct TryOption<T>
    {
        private readonly Exception _exValue;
        public readonly T? Result;

        public TryOption(Exception error)
        {
            _exValue = error;
            Result = default!;
        }

        public TryOption(T result)
        {
            _exValue = HasValueSentinel.Instance;
            Result = result;
        }

        public static TryOption<T> Success(T value)
        {
            return new TryOption<T>(value);
        }

        public static TryOption<T> Error(Exception error)
        {
            return new TryOption<T>(error);
        }

        public bool IsNone()
        {
            return _exValue == null;
        }

        public bool IsError()
        {
            return _exValue != null && !object.ReferenceEquals(_exValue,HasValueSentinel.Instance);
        }
    }

    public readonly record struct CauseReason(
        CauseEnum Cause,
        string CauseName,
        string CauseData);

    public record EventCause
    {
        public CauseEnum Cause { get; init; }
        public string CauseName { get; init; }
        public string CauseData { get; init; }
        public EventParentContext? ParentContext { get; init; }
    }

    public enum CauseEnum
    {
        [EnumMember(Value = "Unknown")]
        Unknown,
        [EnumMember(Value = "Event")]
        Event,
        [EnumMember(Value = "Command")]
        Command
    }

    public static class UpdateAction
    {
        public static UpdateAction<T> Error<T>() => UpdateAction<T>.Error;
        public static UpdateAction<T> Of<T>(T value) => UpdateAction<T>.Of(value);
        public static UpdateAction<T> DoNothing<T>(T value) => UpdateAction<T>.DoNothing;
    }
    public readonly struct UpdateAction<T>
    {
        public static readonly UpdateAction<T> Error = new UpdateAction<T>(0);
        internal readonly int Code;

        private UpdateAction(int code)
        {
            Code = code;
            Value = default!;
        }

        private UpdateAction(int code, T value)
        {
            Code = code;
            Value = value!;
        }

        public static readonly UpdateAction<T> DoNothing =
            new UpdateAction<T>(1);

        internal readonly T Value;

        public static UpdateAction<T> Of(T value)
        {
            return new UpdateAction<T>(2, value);
        }
    }

    public readonly struct UpdateResult<TU>
    {
        public readonly UpdateResultStatus Status;
        public readonly TU? Result;

        internal UpdateResult(UpdateResultStatus status, TU? result)
        {
            Status = status;
            Result = result;
        }
    }
    public enum UpdateResultStatus : byte
    {
        Updated,
        DidNothing,
        Error,
        KeyNotFound,
        KeyDeleted
    }

    public enum DoAck : byte
    {
        DoAck,
        NAck,
        HandleAck,
        StopRedelivery,
        DumpToErrorQueue,
    }

    public static class KVExts
    {
        public static ValueTask<UpdateResult<CausalityWrappedValue<T>>>
            UpdateValue<TState, T>(this NatsKVContext context, TState state,
                string bucketName, string key, CauseReason reason,
                Func<TState, CausalityWrappedValue<T>, ValueTask<UpdateAction<T>>>
                    update, EventParentContext? parentContext)
        {
            return NatsKVWrapper<T>.UpdateValue(state, context, bucketName, key, reason,
                update, parentContext);
        }
    }

    public sealed class NatsKVWrapper<T>
    {
        internal static async ValueTask<UpdateResult<CausalityWrappedValue<T>>>
            UpdateValue<TState>(TState state, NatsKVContext k, string bucketName,
                string key, CauseReason reason,
                Func<TState, CausalityWrappedValue<T>, ValueTask<UpdateAction<T>>>
                    update, EventParentContext? context)
        {
            //Use Revision based update to 'optimistically' perform updates
            var bucket = await k.CreateStoreAsync(bucketName);
            while (true)
            {
                NatsKVEntry<CausalityWrappedValue<T>> e = default;
                try
                {
                    e = await bucket.GetEntryAsync<CausalityWrappedValue<T>>(key);
                }
                catch (NatsKVKeyNotFoundException exception)
                {
                    return new UpdateResult<CausalityWrappedValue<T>>(
                        UpdateResultStatus.KeyNotFound, default);
                }
                catch (NatsKVKeyDeletedException exception)
                {
                    return new UpdateResult<CausalityWrappedValue<T>>(
                        UpdateResultStatus.KeyDeleted, default);
                }

                {
                    var oldVal = e.Value!;
                    var newv = await update(state, oldVal);
                    switch (newv.Code)
                    {
                        case 0:
                            //todo: Error proper
                            return new UpdateResult<CausalityWrappedValue<T>>(
                                UpdateResultStatus.Error, oldVal);
                        case 1:
                            return new UpdateResult<CausalityWrappedValue<T>>(
                                UpdateResultStatus.DidNothing, oldVal);
                        case 2:
                            var forUpdate = oldVal with
                            {
                                Parent = context,
                                Value = newv.Value,
                                Cause = new EventCause(){
                                    Cause = reason.Cause,
                                    CauseData = reason.CauseData,
                                    CauseName = reason.CauseName,
                                    ParentContext = context
                                    
                                }
                            };

                            try
                            {
                                var newRev =
                                    await bucket.UpdateAsync(key, forUpdate,
                                        e.Revision);
                                return new UpdateResult<CausalityWrappedValue<T>>(
                                    UpdateResultStatus.Updated, forUpdate);
                            }
                            catch (NatsKVWrongLastRevisionException concFail)
                            {
                                //inc counter or log
                            }

                            break;
                    }

                }
            }

            await foreach (var b in bucket.WatchAsync<T>(null, null, default))
            {

            }
        }
    }

    public static class NatsJSContextHelpers
    {
        public static async ValueTask<PubAckResponse> PublishForCommand<TCommand, TOut>(
            this NatsJSContext ctx, string subject, TCommand command, TOut outVal, Func<TCommand,EventCause> causeProducer, EventParentContext? context)
        {
            return await ctx.PublishAsync(subject,
                new NatsWrappingEvent<TOut>()
                {
                    Cause = causeProducer(command),
                    Parent = context,
                    Value = outVal
                });
        }
    }

    public record NatsJsWrapper<T>
    {
        private EventParentContext context;

        public NatsJsWrapper(NatsJSMsg<T> msg,
            NatsConnection? errorConnection = null)
        {
            ErrorConnection = errorConnection ?? msg.Connection;
            if (msg.Headers != null)
            {
                Msg = msg;

                if (msg.Metadata.HasValue)
                {
                    var md = msg.Metadata.Value;
                    context = new EventParentContext()
                    {
                        EventContextParentStream = md.Stream,
                        EventContextConsumer = md.Consumer,
                        EventContextSequence = md.Sequence,
                        EventContextTimeStamp = md.Timestamp,
                        EventContextConsumerSequence = md.Sequence.Consumer,
                        EventContextStreamSequence = md.Sequence.Stream,
                        EventContextDomain = md.Domain,
                        EventContextWasResultOfStream =
                            msg.Headers.GetFirstOrNull(
                                "result-of-event-stream"),
                        EventContextWasResultOfEventSeq =
                            msg.Headers.GetFirstOrNull("result-of-event-seq"),
                        EventContextCreatedFrom = msg.Subject
                    };


                }
                else
                {
                    context = new EventParentContext()
                    {
                        EventContextWasResultOfStream =
                            msg.Headers.GetFirstOrNull(
                                "result-of-event-stream"),
                        EventContextWasResultOfEventSeq =
                            msg.Headers.GetFirstOrNull("result-of-event-seq"),
                        EventContextCreatedFrom = msg.Subject,
                    };
                }
            }
        }

        public NatsJSMsg<T> Msg { get; set; }

        public async ValueTask Ack()
        {
            await Msg.AckAsync();
        }

        public async ValueTask NAck()
        {
            await Msg.NakAsync();
        }

        public async ValueTask AckError()
        {
            await Msg.AckTerminateAsync();
        }

        public ValueTask<UpdateResult<CausalityWrappedValue<TU>>>
            UpdateValue<TU>(NatsKVContext k, string bucketName, string key,
                Func<CausalityWrappedValue<TU>, ValueTask<UpdateAction<TU>>>
                    update,
                Func<UpdateResult<CausalityWrappedValue<TU>>, DoAck> ackOption)
        {
            return UpdateValue(update, k, bucketName, key,
                static (a, b) => a(b), ackOption);
        }

        public async ValueTask<PubAckResponse> PublishOutput<TO>(
            NatsJSContext context, string subject, TO output)
        {
            return await context.PublishAsync(subject,
                new NatsWrappingEvent<TO>()
                    { Parent = this.context, Value = output });
        }

        public async ValueTask<CreateResult<CausalityWrappedValue<TU>>>
            CreateValue<TU>(TU state,
                NatsKVContext k, string bucketName, string key, string value,
                EventCause cause)
        {
            var s = await k.GetStoreAsync(bucketName);

            var kv = await s.DoCreate(key,
                new CausalityWrappedValue<TU>()
                    { Value = state, Parent = context, PreviousValue = default, Cause = cause});
            return kv;
        }

        public async ValueTask<UpdateResult<CausalityWrappedValue<TU>>> UpdateValue<
            TState, TU>(
            TState state,
            NatsKVContext k,
            string bucketName,
            string key,
            Func<TState, CausalityWrappedValue<TU>, ValueTask<UpdateAction<TU>>>
                update,
            Func<UpdateResult<CausalityWrappedValue<TU>>, DoAck> ackOption,
            string? causeType = null,
            string? causeData = null
        )
        {
            var res = await NatsKVWrapper<TU>.UpdateValue(state, k, bucketName, key, 
                new CauseReason(CauseEnum.Event, causeType??Msg.Data?.GetType().FullName??string.Empty,causeData??string.Empty)
                ,
                update,
                context);
            var dec = ackOption(res);
            switch (dec)
            {
                case DoAck.StopRedelivery:
                    await Msg.AckTerminateAsync();
                    break;
                case DoAck.HandleAck:
                    await Msg.AckAsync();
                    break;
                case DoAck.NAck:
                    await Msg.NakAsync();
                    break;
                case DoAck.DumpToErrorQueue:
                    await new NatsJSContext((NatsConnection)ErrorConnection!)
                        .PublishAsync(
                            $"error.processing.update.{bucketName}.{key}", "IDK");
                    await Msg.NakAsync();
                    break;
            }

            return res;
        }

        public INatsConnection? ErrorConnection { get; }

    }

    public readonly struct CreateResult<T>
    {
        public CreateResult(CreateResultStatus status, T result)
        {
            Status = status;
            Result = result;
        }
        public readonly CreateResultStatus Status;
        public readonly T Result;
    }

[Flags]
    public enum CreateResultStatus
    {
        Default = 0,
        Created = 1,
        AlreadyExisted = 2,
        DeletedNoRecreate = 4,
        Recreated = 5,
    }