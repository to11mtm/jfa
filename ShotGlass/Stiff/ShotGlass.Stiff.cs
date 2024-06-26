using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Akka;
using Akka.Actor;
using Akka.Actor.Setup;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Stage;
using Akka.Streams.Supervision;
using LanguageExt;
using LinqToDB;
using LinqToDB.Concurrency;
using LinqToDB.Data;
using LinqToDB.Expressions;
using LinqToDB.Mapping;
using LinqToDB.Tools;
using Directive = Akka.Streams.Supervision.Directive;
// ReSharper disable PrivateFieldCanBeConvertedToLocalVariable
// ReSharper disable RedundantArgumentDefaultValue
// ReSharper disable RedundantDefaultMemberInitializer

//So, there's this other project I am working on called 'Venti'.
//It's an event related thing... That's all I will say for now.
//However, I wanted to keep parts of it in the 'GlutenFree' spirit;
//  1. Keep things minimal and hackable
//  2. Prefer Composition of minimal components to a larger whole
// To that end, It's best to start such a project with the storage layer...
// So, this is a 'Small Part' of Venti...
// Thus, the name ShotGlass.
namespace GlutenFree.ShotGlass.Stiff
{
    /// <summary>
    /// This class must be implemented to store the attributes
    /// an implementation sees fit.
    /// <para/>
    /// It does not have an opinion on What attributes should be stored,
    /// Outside of the ones needed for base library functionality.
    /// </summary>
    /// <remarks>
    /// In other words, you can use it like a normal SQL row,
    /// Or you can toss some columns for serialization,
    /// Or a hybrid of the two as needed.
    /// </remarks>
    public abstract class TypedKeyedSequencedRow
    {
        
        [Column(CanBeNull = false, Length = 128)]
        public virtual string RType { get; set; } = null!;

        [Column(CanBeNull = false, Length = 128)]
        public virtual string RKey { get; set; } = null!;
        
        // ReSharper disable once UnusedAutoPropertyAccessor.Global
        public abstract long RSequenceNum { get; set; }
        
        [Column(SkipOnInsert = true)]
        public virtual long ROrdering { get; set; }
        
        /// <summary>
        /// Meant to be a Unix UTC Timestamp in millis. Sane DBs can do this no problem.
        /// </summary>
        [Column(SkipOnInsert= true)]
        public virtual long RInsertTimestamp {get; set; } 
    }
    
    
    public class BatchFlowControl
    {
        public class Continue : BatchFlowControl
        {
            public static Continue Instance = new Continue();
        }

        public class ContinueDelayed : BatchFlowControl
        {
            public static ContinueDelayed Instance = new ContinueDelayed();
        }

        public class Stop : BatchFlowControl
        {
            public static Stop Instance = new Stop();
        }
    }

    public class WriteQueueEntry<T> where T: TypedKeyedSequencedRow
    {
        public List<T> Rows { get; }
        public TaskCompletionSource<Done> Completion { get; }
        public CancellationToken Token { get; }

        public WriteQueueEntry(List<T> rows,
            TaskCompletionSource<Done> completion,
            CancellationToken token)
        {
            Rows = rows;
            Completion = completion;
            Token = token;
        }
    }

    public static class WriteQueueSet
    {

        /// <remarks>
        /// It is the consumer's responsibility to NOT return this to the pool,
        /// <para/>
        /// Or, ensure the pool's return logic (and/or other logic)
        /// will discard it.
        /// <para/>
        /// In our case, The discard logic is tied to our buffer size,
        /// And in the seed function, we just see whether the entry is bigger
        /// or not. If the entry is bigger, The akka streams Batch stage
        /// already is treating it as a snowflake,
        /// And the discard will see it is too big. 
        /// </remarks>
        internal static WriteQueueSet<T> UnPooled<T>(
            this WriteQueueEntry<T> entry) where T: TypedKeyedSequencedRow
        {
            return new WriteQueueSet<T>(entry);
        }
    }

    /// <summary>
    /// Singleton for Reader, Writer, and Sequence Operations.
    /// </summary>
    /// <typeparam name="T">Target Row type.</typeparam>
    /// <remarks>
    /// This can do a lot of magic easily if you are using a single target.
    /// </remarks>
    public class Bartender<T> : IAsyncDisposable
        where T : TypedKeyedSequencedRow
    {
        private readonly ShotGlassRack _rack;
        public readonly ReadReader<T> Reader;
        public readonly BatchQueue<T> Writer;
        public readonly SequenceQueue<T> SequenceGetter;
        
        public Bartender(IDataConnectionFactory connectionFactory,
            IRackMaterializerFactory materializerFactory,
            string? tableName = null)
        {
            _rack = new ShotGlassRack(connectionFactory, materializerFactory);
            Reader = _rack.ReadReader<T>(tableName);

            Writer = _rack.WriteQueue<T>(tableName);
            SequenceGetter = _rack.SequenceQueue<T>(tableName);
        }
        
        /// <param name="connectionFactory">The ConnectionFactory to use</param>
        /// <param name="materializer">
        /// An Akka Streams <see cref="IMaterializer"/>.
        /// If none is provided, a static singleton ActorSystem is created,
        /// in a thread safe fashion.
        /// <param name="tableName"></param>
        public Bartender(IDataConnectionFactory connectionFactory,
            IMaterializer? materializer = null, string? tableName = null)
        {
            if (materializer == null)
            {
                _rack = ShotGlassRack.ViaStatic(connectionFactory);
            }
            else
            {
                _rack = new ShotGlassRack(connectionFactory, materializer);
            }

            //TODO: Confs
            Reader = _rack.ReadReader<T>(tableName);

            Writer = _rack.WriteQueue<T>(tableName);
            SequenceGetter = _rack.SequenceQueue<T>(tableName);
        }

        public async ValueTask DisposeAsync()
        {
            await Writer.DisposeAsync();
            await SequenceGetter.DisposeAsync();
        }
    }

    public class ShotGlassRack
    {
        public ShotGlassRack(IDataConnectionFactory factory,
            IRackMaterializerFactory materializerFactory)
        {
            _factory = factory;
            _materializer = materializerFactory.Materializer;
        }
        public ShotGlassRack(IDataConnectionFactory factory,
            IMaterializer materializer)
        {
            _factory = factory;
            _materializer = materializer;
        }

        public SequenceQueue<T> SequenceQueue<T>(string? tableName = null,
            int maxBatch = 20, int? maxQueue = null, int? maxDop = null) where T : TypedKeyedSequencedRow
        {
            return new SequenceQueue<T>(_factory, _materializer, tableName,
                maxBatch, maxQueue, maxDop);
        }

        public ReadReader<T> ReadReader<T>(string? tableName = null) where T: TypedKeyedSequencedRow
        {
            return new ReadReader<T>(_factory, _materializer, tableName);
        }

        public BatchQueue<T> WriteQueue<T>(string? tableName = null, int bufferSize = 8192, int batchSize = 512, int? maxDop = null)
            where T : TypedKeyedSequencedRow
        {
            return new BatchQueue<T>(_factory, bufferSize, batchSize,
                maxDop.GetValueOrDefault(Environment.ProcessorCount),
                _materializer);
        }
            
        public static ShotGlassRack ViaStatic(IDataConnectionFactory factory)
        {
            if (_staticMaterializerFactory == null)
            {
                lock (StaticFactoryLockObj)
                {
                    if (_staticMaterializerFactory == null)
                    {
                        //TODO: Logger injection, etc.
                        _staticMaterializerFactory = new RackSingletonFactory();
                    }
                }
            }

            return new ShotGlassRack(factory, _staticMaterializerFactory.Materializer);
        }

        private static RackSingletonFactory? _staticMaterializerFactory;
        
        private static readonly object StaticFactoryLockObj = new();
        private readonly IDataConnectionFactory _factory;
        private readonly IMaterializer _materializer;
    }

    public class RackSingletonFactory : IRackMaterializerFactory
    {
        private readonly ActorSystem _system;
        private readonly ActorMaterializer _materializer;

        public RackSingletonFactory(ActorSystemSetup? setup = null)
        {
            if (setup == null)
            {
                _system = ActorSystem.Create("shotglass-rack-static");
            }
            else
            {
                _system = ActorSystem.Create($"shotglass-rack-{setup.GetHashCode()}", setup);
            }
            _materializer = _system.Materializer(namePrefix: "shotglass");
        }

        public IMaterializer Materializer => _materializer;
    }

    public interface IRackMaterializerFactory
    {
        IMaterializer Materializer { get; }
    }
    
    public class WriteQueueSet<T> where T: TypedKeyedSequencedRow
    {
        public readonly List<WriteQueueEntry<T>> Entries;
        public int Count { get; private set; }
        internal WriteQueueSet(WriteQueueEntry<T> seedEntry)
        {
            Entries = new List<WriteQueueEntry<T>>();
            Entries.Add(seedEntry);
            Count = seedEntry.Rows.Count;
        }

        public WriteQueueSet<T> Add(WriteQueueEntry<T> newEntry)
        {
            Entries.Add(newEntry);
            Count = newEntry.Rows.Count;
            return this;
        }
    }

    public interface IDataConnectionFactory
    {
        DataConnection GetConnection();
    }

    public static class BatchQueue
    {
        public static BatchQueue<T>
            AsBatchQueue<T>(this T item,
                IDataConnectionFactory factory, int bufferSize,
                int batchSize, int maxDop, IMaterializer materializer)
            where T : TypedKeyedSequencedRow
        {
            return new BatchQueue<T>(factory, bufferSize,
                batchSize, maxDop, materializer);
        }
    }

    public class KeyAndSequenceFor
    {
        public KeyAndSequenceFor(string key, long seq)
        {
            Key = key;
            Seq = seq;
        }

        public string Key { get; }
        public long Seq { get; }
    }

    public static class L2DbExts
    {
        public static ITable<T> SafeTableName<T>(this ITable<T> table,
            string? tableName) where T : notnull
        {
            if (string.IsNullOrWhiteSpace(tableName))
            {
                return table;
            }

            return table.TableName(tableName);
        }
    }

    public abstract class
        SequenceTrackingChannelReader<TOut> : ChannelReader<TOut>
    {
        public abstract long LastSeq { get; }
    }
    
    public class SequenceTrackingChannelReader<TRecord,TOut> : SequenceTrackingChannelReader<TOut>
        where TRecord : TypedKeyedSequencedRow
    {
        private readonly ChannelReader<Carrier<TRecord>> _reader;
        // This is useful later...
        // ReSharper disable once NotAccessedField.Local
        private long _lastSeq;
        private readonly Func<TRecord, TOut> _map;

        public SequenceTrackingChannelReader(ChannelReader<Carrier<TRecord>> reader, Func<TRecord,TOut> map)
        {
            _reader = reader;
            _map = map;
        }
        public override bool TryRead(out TOut item)
        {
            while (_reader.TryRead(out var cItem) && cItem.CarrierType != CarrierType.Ignore)
            {
                var tItem = cItem.GetValueOrThrow();
                _lastSeq = tItem.RSequenceNum;
                item = _map(tItem);
                return true;
            }

            item = default!;
            return false;
        }

#if NETCOREAPP3_1 || NETSTANDARD2_0
        #else
        public override bool TryPeek(out TOut item)
        {
            if (_reader.TryPeek(out var cItem) &&
                cItem.CarrierType == CarrierType.Event)
            {
                item = _map(cItem.Value);
                return true;
            }

            item = default;
            return false;
        }
#endif

        public override async ValueTask<bool> WaitToReadAsync(
            CancellationToken cancellationToken = new CancellationToken())
        {
            return await _reader.WaitToReadAsync(cancellationToken);
        }

        public override long LastSeq => _lastSeq;
    }
    public class SequenceQueue<T> : IAsyncDisposable
        where T : TypedKeyedSequencedRow
    {
        public readonly string? TableName;
        
        private readonly Task<Done> _completion;

        private int _shutdownState = 0;

        private readonly ChannelWriter<SequenceRequest> _requestQueue;
        private static readonly Expression<Func<T, string>> KeySelector = t=>t.RKey;
        private static readonly Expression<Func<IGrouping<string, T>, KeyAndSequenceFor>> SequenceGroupByExpression = t=>new KeyAndSequenceFor(t.Key,t.Max(i=>i.RSequenceNum));

        /// <summary>
        /// Returns a completion that will be closed once the Queue is shut down.
        /// </summary>
        public async Task ClosedCompletion()
        {
            await _completion;
        }
        
        /// <summary>
        /// Requests completion of the Queue (i.e. for shutdown).
        /// Note that until the result of <see cref="ClosedCompletion"/> completes,
        /// The queue may still be running entries.
        /// </summary>
        /// <param name="error">
        /// Optional: If provided, closes with an error instead of gracefully.
        /// </param>
        /// <returns>
        /// True if this request caused Completion.
        /// <para/>
        /// False if a different request caused completion.
        /// </returns>
        public bool TryFlagCompletion(Exception? error = null)
        {
            if (Interlocked.CompareExchange(ref _shutdownState, 1, 0) == 0)
            {
                _requestQueue.TryComplete(error);
                return true;
            }

            return false;
        }
        public SequenceQueue(IDataConnectionFactory dcf, IMaterializer mat,
            string? tableName = null, int maxBatch = 20, int? maxQueue = null,
            int? maxDop = null)
        {
            TableName = tableName;
            var calcDop =
                maxDop.GetValueOrDefault(Environment.ProcessorCount * 2);
            (_requestQueue, _completion) = Source
                .Channel<SequenceRequest>(
                    maxQueue ?? calcDop * maxBatch, false)
                .GroupBy(16384, sr=>sr.Type)
                .BatchWeightedWithContext(maxBatch,
                    sr => new SequenceRequestGroup(sr.Type,sr.Key,
                        sr.Response),
                    (sr, srg) =>
                    {
                        if (srg != null && srg.Has(sr.Key))
                        {
                            return 0;
                        }

                        return 1;
                    }, (srg, sr) => srg.Add(sr.Key, sr.Response))
                .SelectAsync(calcDop, async srg =>
                {
                    try
                    {
                        using (var ctx = dcf.GetConnection())
                        {
                            var results = await ctx.GetTable<T>()
                                .SafeTableName(TableName)
                                .Where(t=>t.RType==srg.Type && t.RKey.In(srg.Keys))
                                .GroupBy(KeySelector)
                                .Select(SequenceGroupByExpression)
                                .ToListAsync();
                            if (results.Count == srg.Count)
                            {
                                //Fast path; we can set every result easily
                                //And don't need to filter dictionary entries out.
                                foreach (var item in results)
                                {
                                    srg.SetResultsForKey(item.Key, item.Seq);
                                }   
                            }
                            else
                            {
                                srg.SetResultsSlow(results);
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        foreach (var r in srg.Sets)
                        {
                            foreach (var taskCompletionSource in r.Value)
                            {
                                taskCompletionSource.TrySetException(e);
                            }
                        }
                    }

                    return NotUsed.Instance;
                }).MergeSubStreamsAsSource().ToMaterialized(Sink.Ignore<NotUsed>(), Keep.Both).Run(mat);
        }

        public async ValueTask<long> GetSequenceFor(string type, string key,
            CancellationToken token = default)
        {
            var req = new SequenceRequest(type,key);
            await _requestQueue.WriteAsync(req, token);
            return await req.Response.Task;
        }

        public async ValueTask DisposeAsync()
        {
            TryFlagCompletion();
            try
            {
                await _completion;
            }
            catch
            {
                //Intentional
            }
        }
    }

    public static class SubFlowExtensions
    {
        public static Source<TOut, TMat> MergeSubStreamsAsSource<TOut, TMat,
            TClosed>(this SubFlow<TOut, TMat, TClosed> subFlow)
        {
            return (Source<TOut,TMat>)(subFlow.MergeSubstreams());
        }
    }
    
    
    /// <summary>
    /// This is intended to be process-local only
    /// <para/>
    /// AKA we don't care about serialization.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public readonly struct Carrier<T> where T:class
    {
        public Carrier(T value)
        {
            CarrierType = CarrierType.Event;
            _carrierVal = value;
        }

        public static Carrier<T> ForError(Exception exception)
        {
            return new Carrier<T>(exception);
        }
        private Carrier(Exception error)
        {
            CarrierType = CarrierType.Error;
            _carrierVal = error;
        }
        
        public static readonly Carrier<T> Ignore =
            new Carrier<T>(CarrierType.Ignore);
        private readonly object _carrierVal;
        public readonly CarrierType CarrierType;

        private Carrier(CarrierType use)
        {
            _carrierVal = default;
            CarrierType = use;
        }

        public T Value => _carrierVal as T;
        public Exception Error => _carrierVal as Exception;
        public bool IsIgnore => CarrierType == CarrierType.Ignore;
        public T GetValueOrThrow()
        {
            if (CarrierType.HasFlag(CarrierType.Event))
            {
                return Value;
            }
            else if (CarrierType.HasFlag(CarrierType.Error))
            {
                throw Error;
            }
            else
            {
                throw new InvalidOperationException("Invalid state!");
            }
        }
    }

    [Flags]
    public enum CarrierType : byte
    {
        Invalid = 0,
        Ignore = 1,
        Event = 2,
        Error = 4
    }
    internal static class CarrierTypeInternals
    {
        internal static readonly Exception Event = new Exception("event");
        internal static readonly Exception Ignore = new Exception("ignore");
    }

    public class ReadReaderV2<T>
        where T : TypedKeyedSequencedRow
    {
     
        private readonly IDataConnectionFactory _dataConnectionFactory;
        private readonly string? _tableName;
        private readonly Channel<int> callLock;
        public ReadReaderV2(IDataConnectionFactory dataConnectionFactory,
            string? tableName, int maxConcurrent)
        {
            _dataConnectionFactory = dataConnectionFactory;
            _tableName = tableName;
            callLock = Channel.CreateBounded<int>(
                new BoundedChannelOptions(maxConcurrent)
                {
                    AllowSynchronousContinuations = false, SingleReader = false,
                    SingleWriter = false, FullMode = BoundedChannelFullMode.Wait
                });
        }

        public ChannelReader<U> GetMappedSpooler<U>(string type, string key,
            long startAt,
            long? endAt, int maxBatch, Func<T, U> map,
            Func<T, bool>? baseFilter = null,
            CancellationToken token = default)
        {
            var outCh = Channel.CreateBounded<Carrier<T>>(1024);
            Task.Factory.StartNew(async () =>
            {
                var currStart = startAt;
                bool notDone = true;
                try
                {
                    var runQuery = RunQuery(type, key, currStart, endAt,
                        maxBatch);
                    while (notDone)
                    {
                        var results = await runQuery;
                        if (results.Count > 0)
                        {
                            currStart = results[results.Count - 1].RSequenceNum + 1;
                            notDone = currStart > endAt;
                        }
                        else
                        {
                            notDone = false;
                        }

                        if (notDone)
                        {
                            runQuery = RunQuery(type, key, currStart, endAt,
                                maxBatch, true, token);
                        }

                        foreach (var typedKeyedSequencedRow in results)
                        {
                            if (baseFilter == null || baseFilter(typedKeyedSequencedRow))
                            {
                                await outCh.Writer.WriteAsync(
                                    new Carrier<T>(typedKeyedSequencedRow),
                                    token);   
                            }
                        }
                    }

                    outCh.Writer.TryComplete();
                }
                catch (Exception e)
                {
                    outCh.Writer.TryComplete(e);
                }
            }, TaskCreationOptions.LongRunning);
            return new SequenceTrackingChannelReader<T, U>(outCh.Reader, map);
        }

        public async ValueTask<List<T>> RunQuery(string type, string key, long startAt,
            long? argEndAt, int maxBatch, bool forceYield = false, CancellationToken token = default)
        {
            await callLock.Writer.WriteAsync(1, token);
            try
            {
                if (forceYield)
                {
                    await Task.Yield();   
                }
                using (var ctx = _dataConnectionFactory.GetConnection())
                {
                    var query = ctx.GetTable<T>().SafeTableName(_tableName)
                        .Where(t =>
                            t.RType == type && t.RKey == key &&
                            t.RSequenceNum >= startAt);
                    if (argEndAt != null)
                    {
                        query = query.Where(t => t.RSequenceNum <= argEndAt);
                    }

                    return await query.OrderBy(r=>r.RSequenceNum).Take(maxBatch).ToListAsync(token);
                }
            }
            finally
            {
                await callLock.Reader.ReadAsync(default);
            }
        }
    }
    public class ReadReader<T>
        where T : TypedKeyedSequencedRow
    {
        private readonly IDataConnectionFactory _dataConnectionFactory;
        private readonly string? _tableName;

        public ReadReader(IDataConnectionFactory dataConnectionFactory,
            IMaterializer materializer,
            string? tableName)
        {
            _dataConnectionFactory = dataConnectionFactory;
            _tableName = tableName;
            _mat = materializer;
        }

        private IMaterializer _mat;


        public async ValueTask<List<T>> RunQuery(string type, string key, long startAt,
            long? argEndAt, int maxBatch)
        {
            using (var ctx = _dataConnectionFactory.GetConnection())
            {
                var query = ctx.GetTable<T>().SafeTableName(_tableName)
                    .Where(t =>
                        t.RType == type && t.RKey == key &&
                        t.RSequenceNum >= startAt);
                if (argEndAt != null)
                {
                    query = query.Where(t => t.RSequenceNum <= argEndAt);
                }
                return await query.Take(maxBatch).ToListAsync();
            }
        }

        /// <summary>
        /// Spools a set of records out from the DB.
        /// Arguments are batch/buffered to ease overall contention.
        /// </summary>
        /// <param name="type">Type to Query</param>
        /// <param name="key">Key to Query</param>
        /// <param name="startAt">Where to start</param>
        /// <param name="endAt">Where to End</param>
        /// <param name="maxBatch">Max Batch Size</param>
        /// <param name="filter">Filter records out?</param>
        /// <param name="token"></param>
        /// <returns>
        /// A Running Channelreader that will pull records as needed
        /// </returns>
        public ChannelReader<T> GetSpooler(string type, string key, long startAt,
            long? endAt, int maxBatch,
            Func<T,bool>? filter = null,
            CancellationToken token = default)
        {
            return GetMappedSpooler(type, key, startAt, endAt, maxBatch, t => t,
                filter,
                token);
        }

        /// <summary>
        /// Returns a Mapped Spooler.
        /// </summary>
        /// <param name="type"></param>
        /// <param name="key"></param>
        /// <param name="startAt"></param>
        /// <param name="endAt"></param>
        /// <param name="maxBatch"></param>
        /// <param name="map"></param>
        /// <param name="baseFilter"></param>
        /// <param name="token"></param>
        /// <typeparam name="U"></typeparam>
        /// <remarks>
        /// A mapped spooler allows a custom mapping of read records
        /// Into an in-appmem series of events.
        /// </remarks>
        public ChannelReader<U> GetMappedSpooler<U>(string type,string key, long startAt,
            long? endAt, int maxBatch, Func<T, U> map, Func<T,bool>? baseFilter = null,
            CancellationToken token = default)
        {
            if (baseFilter == null)
            {
                baseFilter = static t => true;
            }
            
            return new SequenceTrackingChannelReader<T,U>(Source.UnfoldAsync<spoolCapt,Carrier<List<T>>>(
                    new spoolCapt(type,key, startAt, endAt, maxBatch),
                    async (c) =>
                    {
                        try
                        {
                            token.ThrowIfCancellationRequested();
                            if (c.startAt <= c.endAt)
                            {
                                var results = await RunQuery(c.type,c.key, c.startAt,
                                    c.endAt,
                                    c.maxBatch);
                                if (results.Count > 0)
                                {
                                    return Akka.Util.Option<(spoolCapt,Carrier<List<T>>)>.Create((new spoolCapt(c.type,c.key,
                                        results[results.Count-1].RSequenceNum+1,
                                        c.endAt,
                                        c.maxBatch), new Carrier<List<T>>(results)));
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            return Akka.Util
                                .Option<(spoolCapt, Carrier<List<T>>)>.Create((
                                    new spoolCapt(c.type, c.key, long.MaxValue,
                                        long.MinValue, 0),
                                    Carrier<List<T>>.ForError(e)));
                        }
                        

                        return Akka.Util.Option<(spoolCapt, Carrier<List<T>>)>.None;
                    }).SelectMany(a =>
                {
                    if ((a.CarrierType & CarrierType.Event) != 0)
                    {
                        return a.Value.Select(b=>new Carrier<T>(b));
                    }
                    else
                    {
                        return new List<Carrier<T>>()
                            { Carrier<T>.ForError(a.Error) };
                    }
                }).Where(a=> (a.CarrierType.HasFlag(CarrierType.Event) && baseFilter(a.Value)) || a.CarrierType.HasFlag(CarrierType.Error))
                .RunWith(
                    ChannelSink.AsReader<Carrier<T>>(32, false,
                        BoundedChannelFullMode.Wait),
                    _mat),map);
        }

        internal readonly struct spoolCapt
        {
            public readonly string type;
            public readonly string key;
            public readonly long startAt;
            public readonly long? endAt;
            public readonly int maxBatch;

            public spoolCapt(string type, string key, long startAt, long? endAt, int maxBatch)
            {
                this.type = type;
                this.key = key;
                this.startAt = startAt;
                this.endAt = endAt;
                this.maxBatch = maxBatch;
            }
        }
    }

    public class SequenceRequestGroup
    {
        private readonly Dictionary<string, List<TaskCompletionSource<long>>>
            _reqSet;

        public readonly string Type;
        public SequenceRequestGroup(string type, string key, TaskCompletionSource<long> req)
        {
            Type = type;
            _reqSet = new Dictionary<string, List<TaskCompletionSource<long>>>();
            _reqSet.Add(key, new List<TaskCompletionSource<long>>() { req });
        }

        public bool Has(string key)
        {
            return _reqSet.ContainsKey(key);
        }

        public IEnumerable<string> Keys => _reqSet.Keys;

        public IEnumerable<KeyValuePair<string, List<TaskCompletionSource<long>>>>
            Sets => _reqSet;

        public int Count => _reqSet.Count;

        internal void SetResultsForKey(string key, long item)
        {
            if (_reqSet.TryGetValue(key, out var _set))
            {
                foreach (var taskCompletionSource in _set)
                {
                    taskCompletionSource.TrySetResult(item);
                }
            }
        }

        internal void SetResultsSlow(List<KeyAndSequenceFor> set)
        {
            foreach (var item in set)
            {
                if (_reqSet.TryGetValue(item.Key, out var entry))
                {
                    foreach (var tcs in entry)
                    {
                        tcs.TrySetResult(item.Seq);
                    }

                    _reqSet.Remove(item.Key);
                }
            }

            foreach (var keyValuePair in _reqSet)
            {
                foreach (var tcs in keyValuePair.Value)
                {
                    tcs.TrySetResult(0);
                }
            }
        }

        public SequenceRequestGroup Add(string key,
            TaskCompletionSource<long> req)
        {
            if (_reqSet.TryGetValue(key, out var set))
            {
                set.Add(req);
            }
            else
            {
                _reqSet.Add(key, new List<TaskCompletionSource<long>>() { req });
            }

            return this;
        }
    }

    public static class StreamDsl
    {
        public static Source<TOut2, TMat>
            BatchWeightedWithContext<TOut, TOut2, TMat>(
                this Source<TOut, TMat> flow, long max, Func<TOut, TOut2> seed,
                Func<TOut, TOut2?, long> costFunction,
                Func<TOut2, TOut, TOut2> aggregate)
        {
            return (Source<TOut2, TMat>)flow.Via(
                new BatchWeightWithContext<TOut, TOut2>(max, costFunction,
                    seed, aggregate));
        }
        
        public static SubFlow<TOut2, TMat, TClosed> BatchWeightedWithContext<TOut, TOut2, TMat,TClosed>(
                this SubFlow<TOut, TMat,TClosed> flow, long max, Func<TOut, TOut2> seed,
                Func<TOut, TOut2?, long> costFunction,
                Func<TOut2, TOut, TOut2> aggregate)
        {
            return (SubFlow<TOut2, TMat,TClosed>)flow.Via(
                new BatchWeightWithContext<TOut, TOut2>(max, costFunction,
                    seed, aggregate));
        }
    }

    #region streams hackery
    public sealed class
        BatchWeightWithContext<TIn, TOut> : GraphStage<FlowShape<TIn, TOut>>
    {
        #region internal classes

        private sealed class Logic : InAndOutGraphStageLogic
        {
            private readonly FlowShape<TIn, TOut> _shape;
            private readonly BatchWeightWithContext<TIn, TOut> _stage;
            private readonly Akka.Streams.Supervision.Decider _decider;
            private Akka.Util.Option<TOut> _aggregate;
            private long _left;
            private Akka.Util.Option<TIn> _pending;

            public Logic(Attributes inheritedAttributes,
                BatchWeightWithContext<TIn, TOut> stage) : base(stage.Shape)
            {
                _shape = stage.Shape;
                _stage = stage;
                var attr = inheritedAttributes
                    .GetAttribute<ActorAttributes.SupervisionStrategy>(null);
                _decider = attr != null
                    ? attr.Decider
                    : Deciders.StoppingDecider;
                _left = stage._max;

                SetHandlers(_shape.Inlet, _shape.Outlet, this);
            }

            public override void OnPush()
            { 
                var element = Grab(_shape.Inlet);
                var cost =
                    _stage._costFunc(element, _aggregate.GetOrElse(default));
                if (!_aggregate.HasValue)
                {
                    try
                    {
                        _aggregate = _stage._seed(element);
                        _left -= cost;
                    }
                    catch (Exception ex)
                    {
                        switch (_decider(ex))
                        {
                            case Directive.Stop:
                                FailStage(ex);
                                break;
                            case Directive.Restart:
                                RestartState();
                                break;
                            case Directive.Resume:
                                break;
                        }
                    }
                }
                else if (_left < cost)
                    _pending = element;
                else
                {
                    try
                    {
                        _aggregate =
                            _stage._aggregate(_aggregate.Value, element);
                        _left -= cost;
                    }
                    catch (Exception ex)
                    {
                        switch (_decider(ex))
                        {
                            case Directive.Stop:
                                FailStage(ex);
                                break;
                            case Directive.Restart:
                                RestartState();
                                break;
                            case Directive.Resume:
                                break;
                        }
                    }
                }

                if (IsAvailable(_shape.Outlet))
                    Flush();
                if (!_pending.HasValue)
                    Pull(_shape.Inlet);
            }

            public override void OnUpstreamFinish()
            {
                if (!_aggregate.HasValue)
                    CompleteStage();
            }

            public override void OnPull()
            {
                if (!_aggregate.HasValue)
                {
                    if (IsClosed(_shape.Inlet))
                        CompleteStage();
                    else if (!HasBeenPulled(_shape.Inlet))
                        Pull(_shape.Inlet);
                }
                else if (IsClosed(_shape.Inlet))
                {
                    Push(_shape.Outlet, _aggregate.Value);
                    if (!_pending.HasValue)
                        CompleteStage();
                    else
                    {
                        try
                        {
                            _aggregate = _stage._seed(_pending.Value);
                        }
                        catch (Exception ex)
                        {
                            switch (_decider(ex))
                            {
                                case Directive.Stop:
                                    FailStage(ex);
                                    break;
                                case Directive.Restart:
                                    RestartState();
                                    if (!HasBeenPulled(_shape.Inlet))
                                        Pull(_shape.Inlet);
                                    break;
                                case Directive.Resume:
                                    break;
                            }
                        }

                        _pending = Akka.Util.Option<TIn>.None;
                    }
                }
                else
                {
                    Flush();
                    if (!HasBeenPulled(_shape.Inlet))
                        Pull(_shape.Inlet);
                }
            }

            private void Flush()
            {
                if (_aggregate.HasValue)
                {
                    Push(_shape.Outlet, _aggregate.Value);
                    _left = _stage._max;
                }

                if (_pending.HasValue)
                {
                    try
                    {
                        _aggregate = _stage._seed(_pending.Value);
                        _left -= _stage._costFunc(_pending.Value, default);
                        _pending = Akka.Util.Option<TIn>.None;
                    }
                    catch (Exception ex)
                    {
                        switch (_decider(ex))
                        {
                            case Directive.Stop:
                                FailStage(ex);
                                break;
                            case Directive.Restart:
                                RestartState();
                                break;
                            case Directive.Resume:
                                _pending = Akka.Util.Option<TIn>.None;
                                break;
                        }
                    }
                }
                else
                    _aggregate = Akka.Util.Option<TOut>.None;
            }

            public override void PreStart() => Pull(_shape.Inlet);

            private void RestartState()
            {
                _aggregate = Akka.Util.Option<TOut>.None;
                _left = _stage._max;
                _pending = Akka.Util.Option<TIn>.None;
            }
        }

        #endregion

        private readonly long _max;
        private readonly Func<TIn, TOut?, long> _costFunc;
        private readonly Func<TIn, TOut> _seed;
        private readonly Func<TOut, TIn, TOut> _aggregate;

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="max">TBD</param>
        /// <param name="costFunc">TBD</param>
        /// <param name="seed">TBD</param>
        /// <param name="aggregate">TBD</param>
        public BatchWeightWithContext(long max, Func<TIn, TOut?, long> costFunc,
            Func<TIn, TOut> seed, Func<TOut, TIn, TOut> aggregate)
        {
            _max = max;
            _costFunc = costFunc;
            _seed = seed;
            _aggregate = aggregate;

            var inlet = new Inlet<TIn>("Batch.in");
            var outlet = new Outlet<TOut>("Batch.out");

            Shape = new FlowShape<TIn, TOut>(inlet, outlet);
        }

        /// <summary>
        /// TBD
        /// </summary>
        public override FlowShape<TIn, TOut> Shape { get; }

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="inheritedAttributes">TBD</param>
        /// <returns>TBD</returns>
        protected override GraphStageLogic CreateLogic(
            Attributes inheritedAttributes)
            => new Logic(inheritedAttributes, this);
    }
#endregion
    public sealed class SequenceRequest
    {
        public SequenceRequest(string type, string key)
        {
            Type = type;
            Key = key;
            Response = new TaskCompletionSource<long>(TaskCreationOptions
                .RunContinuationsAsynchronously);
        }

        public TaskCompletionSource<long> Response { get; }
        public string Type { get; }
        public string Key { get; }
    }

    public class BatchQueue<T> : IAsyncDisposable
        where T : TypedKeyedSequencedRow
    {
        private readonly ChannelWriter<WriteQueueEntry<T>> WriteQueue;
        private readonly IDataConnectionFactory _factory;
        private readonly string TableName;
        private readonly IMaterializer Materializer;
        private Task<Done> Completion;

        public async ValueTask WriteJournalRowsAsync(List<T> rowSet,
            CancellationToken token = default)
        {
            using (var ctx = _factory.GetConnection())
            {
                if (rowSet.Count == 1)
                {
                    await ctx.InsertAsync(rowSet[0], TableName, token: token);
                }
                else
                {
                    await InsertMulti(rowSet, token, ctx);
                }
            }
        }

        private async Task InsertMulti(List<T> rowSet, CancellationToken token,
            DataConnection ctx)
        {
            using (var tx = await ctx.BeginTransactionAsync(token))
            {
                try
                {
                    await ctx.GetTable<T>()
                        .BulkCopyAsync(
                            new BulkCopyOptions(
                                TableName: this.TableName,
                                BulkCopyType: BulkCopyType
                                    .MultipleRows), rowSet, token);
                    await tx.CommitAsync(token);
                }
                catch (Exception e)
                {
                    try
                    {
                        // We should always try to rollback even if cancelled.
                        // ReSharper disable once MethodSupportsCancellation
                        await tx.RollbackAsync();
                    }
                    catch (Exception exception)
                    {
                        throw new AggregateException(e, exception);
                    }
                }
            }
        }

        public BatchQueue(IDataConnectionFactory factory, int bufferSize,
            int batchSize, int maxDop, IMaterializer materializer)
        {
            Materializer = materializer;
            _factory = factory;
            (WriteQueue,Completion) =  Source
                .Channel<WriteQueueEntry<T>>(bufferSize, false,
                    BoundedChannelFullMode.Wait)
                .BatchWeighted(
                    batchSize,
                    cf => cf.Rows.Count,
                    r => r.UnPooled(),
                    (oldRows, newRows) =>
                        oldRows.Add(newRows))
                .SelectAsync(
                    maxDop,
                    async promisesAndRows =>
                    {
                        //Hack: We use a Yield here to guarantee parallelism.
                        //Otherwise,
                        //Grabbing a connection may block but not yield
                        //(In some cases)
                        await Task.Yield();
                        try
                        {
                            List<T> writeSet = new List<T>();
                            var entrySet = promisesAndRows.Entries;
                            //Go -backwards- here,
                            //Otherwise Remove has pathological copy behavior.
                            for (var i = entrySet.Count-1;
                                 i > 0;
                                 i--)
                            {
                                var t = entrySet[i];
                                if (t.Token.IsCancellationRequested)
                                {
                                    t.Completion.TrySetCanceled(t.Token);
                                    entrySet.RemoveAt(i);
                                }
                                else
                                {
                                    writeSet.AddRange(t.Rows);
                                }
                            }
                            await WriteJournalRowsAsync(writeSet);
                            foreach (var e in entrySet)
                                e.Completion.TrySetResult(Done.Instance);
                        }
                        catch (Exception e)
                        {
                            foreach (var errSet in promisesAndRows.Entries)
                                errSet.Completion.TrySetException(e);
                        }
                        finally
                        {
                            
                        }

                        return NotUsed.Instance;
                    })
                .ToMaterialized(
                    Sink.Ignore<NotUsed>(),
                    Keep.Both).Run(Materializer);
        }

        public ValueTask WriteAsync(T item,
            CancellationToken token = default)
        {
            return WriteAsync(new List<T>(1) { item }, token);
        }

        /// <remarks>
        /// We use ValueTask here despite a TCS,
        /// with the intent of allowing pooling in future. 
        /// </remarks>
        public async ValueTask WriteAsync(List<T> items,
            CancellationToken token = default)
        {
            var entry = new WriteQueueEntry<T>(items,
                new TaskCompletionSource<Done>(TaskCreationOptions
                    .RunContinuationsAsynchronously),token);
            if (!WriteQueue.TryWrite(entry))
            {
                await WriteQueue.WriteAsync(entry, token);
            }

            await entry.Completion.Task;
        }

        public async Task<Akka.Util.Try<Done>> Closed()
        {
            try
            {
                await Completion.ConfigureAwait(false);
                return new Akka.Util.Try<Done>(Done.Instance);
            }
            catch (Exception e)
            {
                return new Akka.Util.Try<Done>(e);
            }
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                WriteQueue.TryComplete();
            }
            catch
            {
                //Intentional
            }

            try
            {
                await Completion;
            }
            catch
            {
                //Intentional
            }
        }
    }
}