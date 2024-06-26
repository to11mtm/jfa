using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Akka;
using Akka.Actor;
using Akka.Pattern;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Stage;
using Akka.Streams.Supervision;
using LanguageExt;
using LinqToDB;
using LinqToDB.Data;
using LinqToDB.Expressions;
using LinqToDB.Tools;
using Microsoft.Extensions.ObjectPool;
using Directive = Akka.Streams.Supervision.Directive;

namespace ShotGlass
{
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

    public class WriteQueueEntry<T>
    {
        public List<T> Rows { get; }
        public TaskCompletionSource<Done> Completion { get; }

        public WriteQueueEntry(List<T> rows,
            TaskCompletionSource<Done> completion)
        {
            Rows = rows;
            Completion = completion;
        }
    }

    public static class WriteQueueSet
    {
        public static WriteQueueSet<T> ToSetFromPool<T>(
            this ObjectPool<List<T>> pool, WriteQueueEntry<T> entry)
        {
            var p = pool.Get();
            p.AddRange(entry.Rows);
            return new WriteQueueSet<T>(p, entry.Completion);
        }

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
            this WriteQueueEntry<T> entry)
        {
            return new WriteQueueSet<T>(entry.Rows, entry.Completion);
        }
    }

    public class WriteQueueSet<T>
    {
        public List<T> Rows { get; }
        public List<TaskCompletionSource<Done>> Completions { get; }

        internal WriteQueueSet(List<T> rows,
            TaskCompletionSource<Done> completion)
        {
            Rows = rows;
            Completions = new List<TaskCompletionSource<Done>>() { completion };
        }

        public WriteQueueSet<T> Add(List<T> rows,
            TaskCompletionSource<Done> completion)
        {
            Rows.AddRange(rows);
            Completions.Add(completion);
            return this;
        }
    }

    public class InstancePooledObjectPolicy<T> : IPooledObjectPolicy<List<T>>
    {
        public readonly int MaxSize;

        public InstancePooledObjectPolicy(int maxSize)
        {
            MaxSize = maxSize;
            CreateSize = Math.Max(32, Math.Min(maxSize / 4, 64));
        }

        public readonly int CreateSize;

        public List<T> Create()
        {
            return new List<T>(CreateSize);
        }

        public bool Return(List<T> obj)
        {
            var retVal = obj.Count <= MaxSize;
            obj.Clear();
            return retVal;
        }
    }

    public interface IDataConnectionFactory
    {
        DataConnection GetConnection();
    }

    public interface IUniquelySortable<TKey>
    {
        TKey Key { get; set; }
        long Seq { get; set; }
    }

    public static class BatchQueue
    {
        public static BatchQueue<T>
            AsBatchQueue<T, TKey>(this T item,
                IDataConnectionFactory factory, int bufferSize,
                int batchSize, int maxDop, IMaterializer materializer)
            where T : class, IUniquelySortable<TKey>
        {
            return new BatchQueue<T>(factory, bufferSize,
                batchSize, maxDop, materializer);
        }
    }

    public class KeyAndSequenceFor<TKey>
    {
        public KeyAndSequenceFor(TKey key, long seq)
        {
            Key = key;
            Seq = seq;
        }

        public TKey Key { get; }
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

            return table.SafeTableName(tableName!);
        }
    }

    public class SequenceQueue<T, TKey> where T : class
    {
        private readonly string? TableName;
        
        private static readonly MethodInfo InCall = MemberHelper.MethodOf(() =>
            SqlExtensions.In((TKey)default!, (IEnumerable<TKey>)default!));
        private static Expression<Func<T, bool>> BuildPredicate(
            IEnumerable<TKey> keys, Expression<Func<T, TKey>> keySel)
        {
            var p = Expression.Parameter(typeof(T));
            return Expression.Lambda<Func<T, bool>>(Expression.Call(null,InCall,
                Expression.Invoke(keySel, p),Expression.Constant(keys)), p);
        }

        private static
            Expression<Func<IGrouping<TKey, T>, KeyAndSequenceFor<TKey>>>
            BuildGrouping(
                Expression<Func<T, long>> seqSel)
        {
            var p = Expression.Parameter(typeof(IGrouping<TKey, T>));
            return Expression
                .Lambda<Func<IGrouping<TKey, T>, KeyAndSequenceFor<TKey>>>(
                    Expression.New(
                        typeof(KeyAndSequenceFor<TKey>).GetConstructor(
                            new[]
                                { typeof(TKey) })!,
                        Expression.Invoke(GroupKeySel, p),
                        Expression.Invoke(seqSel, p)), p);
        }

        private static readonly Expression<Func<IGrouping<TKey, T>, TKey>>
            GroupKeySel = (g => g.Key);

        private readonly Task<Done> _completion;
        private readonly ChannelWriter<SequenceRequest<TKey>> _requestQueue;

        //Max<TSource>(this IEnumerable<TSource> source, Func<TSource, int?> selector) => MaxInteger(source, selector)
        private static readonly MethodInfo MaxMethod = MemberHelper.MethodOf(() =>
            Enumerable.Max((IEnumerable<T>)default!, (Func<T, long>)default!));
        public static Expression<Func<IEnumerable<T>, long>> MaxExpr(
            Expression<Func<IEnumerable<T>, long>> seqSel)
        {
            var method = MaxMethod;
            var p = Expression.Parameter(typeof(T));
            return Expression.Lambda<Func<IEnumerable<T>, long>>(
                Expression.Call(method, p, seqSel));
        }

        public async Task Closed()
        {
            await _completion;
        }
        public SequenceQueue(IDataConnectionFactory dcf, IMaterializer mat,
            Expression<Func<T, TKey>> keySel, Expression<Func<T, long>> seqSel,
            string? tableName = null, int maxBatch = 20, int? maxQueue = null,
            int? maxDop = null)
        {
            TableName = tableName;
            var _maxDop =
                maxDop.GetValueOrDefault(Environment.ProcessorCount * 2);
            (_requestQueue, _completion) = Source
                .Channel<SequenceRequest<TKey>>(
                    maxQueue ?? _maxDop * maxBatch, false)
                .BatchWeightedWithContext(maxBatch,
                    sr => new SequenceRequestGroup<TKey>(sr.Key,
                        sr.Response),
                    (sr, srg) =>
                    {
                        if (srg != null && srg.Has(sr.Key))
                        {
                            return 0;
                        }

                        return 1;
                    }, (srg, sr) => srg.Add(sr.Key, sr.Response))
                .SelectAsync(_maxDop, async srg =>
                {
                    try
                    {
                        using (var ctx = dcf.GetConnection())
                        {
                            var results = await ctx.GetTable<T>()
                                .SafeTableName(TableName)
                                .Where(BuildPredicate(srg.Keys, keySel))
                                .GroupBy(keySel)
                                .Select(BuildGrouping(seqSel))
                                .ToListAsync();
                            foreach (var item in results)
                            {
                                srg.SetResultsForKey(item.Key, item.Seq);
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
                }).ToMaterialized(Sink.Ignore<NotUsed>(), Keep.Both).Run(mat);
        }

        public async ValueTask<long> GetSequenceFor(TKey key,
            CancellationToken token = default)
        {
            var req = new SequenceRequest<TKey>(key);
            await _requestQueue.WriteAsync(req, token);
            return await req.Response.Task;
        }
    }

    public class ReadReader<T, TKey> where T : class
    {
        private readonly IDataConnectionFactory _dataConnectionFactory;
        private readonly string _tableName;
        private readonly Expression<Func<T, TKey>> _keySel;
        private readonly Expression<Func<T, long>> _seqSel;
        private readonly Func<T, long> _seqComp;

        public ReadReader(IDataConnectionFactory dataConnectionFactory,
            IMaterializer materializer,
            string tableName, Expression<Func<T, TKey>> keySel,
            Expression<Func<T, long>> seqSel)
        {
            _dataConnectionFactory = dataConnectionFactory;
            _tableName = tableName;
            _keySel = keySel;
            _seqSel = seqSel;
            _seqComp = _seqSel.Compile();
            _mat = materializer;
        }

        static Expression<Func<T, bool>> BuildPredicate2(
            TKey keys, Expression<Func<T, TKey>> keySel,
            Expression<Func<T, long>> seqSel, long min, long? max)
        {
            var p = Expression.Parameter(typeof(T));
            var capture = Expression.Constant(new predicateCapture()
                { key = keys, maxSeq = max.GetValueOrDefault(), minSeq = min });
            Expression fKeySel = TryReduce(keySel, p);
            Expression fSeqSel = TryReduce(seqSel, p);
            if (max != null)
            {
                var btCall = Expression.Call(null, BetweenMethodInfo,
                    fSeqSel,
                    Expression.Field(capture,
                        nameof(predicateCapture.minSeq)),
                    Expression.Field(capture,
                        nameof(predicateCapture.maxSeq))
                );
                return Expression.Lambda<Func<T, bool>>(
                    Expression.And(Expression.Equal(
                        Expression.Field(capture,
                            nameof(predicateCapture.key)),
                        fKeySel), btCall), p);
            }
            else
            {
                return Expression.Lambda<Func<T, bool>>(
                    Expression.And(Expression.Equal(
                            Expression.Field(capture,
                                nameof(predicateCapture.key)),
                            fKeySel),
                        Expression.GreaterThanOrEqual(fSeqSel,
                            Expression.Field(capture,
                                nameof(predicateCapture.minSeq)))), p);
            }
        }

        public class predicateCapture
        {
            public TKey key;
            public long minSeq;
            public long maxSeq;
        }

        static Expression TryReduce<TSel>(Expression<Func<T, TSel>> selector,
            Expression arg)
        {
            Expression finalSel = null;
            if (selector.Body is MemberExpression me &&
                me.Expression?.NodeType == ExpressionType.Parameter
                && me.Member?.DeclaringType?.IsAssignableFrom(typeof(T)) ==
                true)
            {
                finalSel = me.Update(arg);
            }
            else
            {
                finalSel = Expression.Invoke(selector, arg);
            }

            return finalSel;
        }

        private readonly Expression<Func<T, bool>> _queryPredExpr;
        private IMaterializer _mat;

        private static readonly MethodInfo BetweenMethodInfo = MemberHelper.MethodOf(() =>
            LinqToDB.Sql.Between<long>(default, default, default));

        public async ValueTask<List<T>> RunQuery(TKey key, long min,
            long? argEndAt, int maxBatch)
        {
            using (var ctx = _dataConnectionFactory.GetConnection())
            {
                return await ctx.GetTable<T>().SafeTableName(_tableName)
                    .Where(
                        BuildPredicate2(key, _keySel, _seqSel, min, argEndAt))
                    .Take(maxBatch).ToListAsync();
            }
        }

        public ChannelReader<T> GetSpooler(TKey key, long startAt,
            long? endAt, int maxBatch,
            CancellationToken token = default)
        {
            return GetMappedSpooler(key, startAt, endAt, maxBatch, t => t,
                token);
        }

        public ChannelReader<U> GetMappedSpooler<U>(TKey key, long startAt,
            long? endAt, int maxBatch, Func<T, U> map,
            CancellationToken token = default)
        {
            return Source.UnfoldAsync(
                    new spoolCapt(key, startAt, endAt, maxBatch),
                    async (c) =>
                    {
                        token.ThrowIfCancellationRequested();
                        if (startAt <= endAt)
                        {
                            var results = await RunQuery(c.key, c.startAt,
                                c.endAt,
                                c.maxBatch);
                            if (results.Count > 0)
                            {
                                return (new spoolCapt(c.key,
                                    _seqComp(results[results.Count - 1]) + 1,
                                    c.endAt,
                                    c.maxBatch), results);
                            }
                        }

                        return Akka.Util.Option<(spoolCapt, List<T>)>.None;
                    }).SelectMany(a => a)
                .Via(token.AsFlow<T>())
                .Select(map)
                .RunWith(
                    ChannelSink.AsReader<U>(32, false,
                        BoundedChannelFullMode.Wait),
                    _mat);
        }

        private readonly struct spoolCapt
        {
            public readonly TKey key;
            public readonly long startAt;
            public readonly long? endAt;
            public readonly int maxBatch;

            public spoolCapt(TKey key, long startAt, long? endAt, int maxBatch)
            {
                this.key = key;
                this.startAt = startAt;
                this.endAt = endAt;
                this.maxBatch = maxBatch;
            }
        }
    }

    public class SequenceRequestGroup<TKey>
    {
        private readonly Dictionary<TKey, List<TaskCompletionSource<long>>>
            ReqSet;

        public SequenceRequestGroup(TKey key, TaskCompletionSource<long> req)
        {
            ReqSet = new Dictionary<TKey, List<TaskCompletionSource<long>>>();
            ReqSet.Add(key, new List<TaskCompletionSource<long>>() { req });
        }

        public bool Has(TKey key)
        {
            return ReqSet.ContainsKey(key);
        }

        public IEnumerable<TKey> Keys => ReqSet.Keys;

        public IEnumerable<KeyValuePair<TKey, List<TaskCompletionSource<long>>>>
            Sets => ReqSet;

        internal void SetResultsForKey(TKey key, long item)
        {
            if (ReqSet.TryGetValue(key, out var _set))
            {
                foreach (var taskCompletionSource in _set)
                {
                    taskCompletionSource.TrySetResult(item);
                }
            }
        }

        public SequenceRequestGroup<TKey> Add(TKey key,
            TaskCompletionSource<long> req)
        {
            if (ReqSet.TryGetValue(key, out var set))
            {
                set.Add(req);
            }
            else
            {
                ReqSet.Add(key, new List<TaskCompletionSource<long>>() { req });
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
    }

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

    public class SequenceRequest<TKey>
    {
        public SequenceRequest(TKey key)
        {
            Key = key;
            Response = new TaskCompletionSource<long>(TaskCreationOptions
                .RunContinuationsAsynchronously);
        }

        public TaskCompletionSource<long> Response { get; }

        public TKey Key { get; }
    }

    public class BatchQueue<T> where T : class
    {
        private readonly ChannelWriter<WriteQueueEntry<T>> WriteQueue;
        private readonly ObjectPool<List<T>> WriteEntryPool;
        private readonly IDataConnectionFactory _factory;
        private readonly string TableName;
        private readonly IMaterializer Materializer;
        private Task<Done> Completion;

        public async ValueTask WriteJournalRowsAsync(List<T> rowset,
            CancellationToken token = default)
        {
            using (var ctx = _factory.GetConnection())
            {
                if (rowset.Count == 1)
                {
                    await ctx.InsertAsync(rowset[0], TableName, token: token);
                }
                else
                {
                    await InsertMulti(rowset, token, ctx);
                }
            }
        }

        private async Task InsertMulti(List<T> rowset, CancellationToken token,
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
                                    .MultipleRows), rowset, token);
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
            WriteEntryPool =
                new DefaultObjectPool<List<T>>(
                    new InstancePooledObjectPolicy<T>(batchSize), 32);
            (WriteQueue, Completion) = Source
                .Channel<WriteQueueEntry<T>>(bufferSize, false,
                    BoundedChannelFullMode.Wait)
                .BatchWeighted(
                    batchSize,
                    cf => cf.Rows.Count,
                    r => r.Rows.Count > batchSize
                        ? r.UnPooled()
                        : WriteEntryPool.ToSetFromPool(r),
                    (oldRows, newRows) =>
                        oldRows.Add(newRows.Rows, newRows.Completion))
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
                            await WriteJournalRowsAsync(promisesAndRows.Rows);
                            foreach (var taskCompletionSource in promisesAndRows
                                         .Completions)
                                taskCompletionSource
                                    .TrySetResult(Done.Instance);
                        }
                        catch (Exception e)
                        {
                            foreach (var taskCompletionSource in promisesAndRows
                                         .Completions)
                                taskCompletionSource.TrySetException(e);
                        }
                        finally
                        {
                            WriteEntryPool.Return(promisesAndRows.Rows);
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
                    .RunContinuationsAsynchronously));
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
    }

    public static class BatchStream
    {
        /// <summary>
        /// Creates a Source that will read in batches
        /// </summary>
        /// <param name="startAt">The starting point</param>
        /// <param name="endAt">The end point</param>
        /// <param name="batchSize">number of records expected in batch</param>
        /// <param name="batchProducer">
        ///   A function to produce a batch of records given a start and end
        ///   This function should return records such that the last returned
        ///   Has the 'last' record based on the sequencenumber
        /// </param>
        /// <param name="getSequenceNumber">
        ///   A function to retrieve the sequence number after each function,
        ///   Expected to return
        /// </param>
        /// <param name="refreshInterval">
        /// If provided, this may be used to continually 'poll'
        /// until <see cref="endAt"/> has been reached,
        /// Rather than completing as soon as there are no more records to return
        /// </param>
        /// <typeparam name="TElem"></typeparam>
        /// <returns></returns>
        public static Source<TElem, NotUsed> Create<TElem>(
            long startAt,
            long endAt,
            int batchSize,
            Func<(long startBatchAt, long endBatchAt, int batchSize),
                Task<Seq<TElem>>> batchProducer,
            Func<TElem, long> getSequenceNumber,
            Akka.Util.Option<(TimeSpan, IScheduler)> refreshInterval
        ) =>
            Source
                .UnfoldAsync<(long, BatchFlowControl),
                    Seq<TElem>>(
                    (Math.Max(1, startAt),
                        BatchFlowControl.Continue.Instance),
                    async opt =>
                    {
                        async Task<Akka.Util.Option<((long, BatchFlowControl),
                                Seq<TElem>)>>
                            RetrieveNextBatch()
                        {
                            Seq<TElem> msg;
                            msg = await batchProducer((opt.Item1, endAt,
                                batchSize));

                            var hasMoreEvents = msg.Count == batchSize;
                            var lastMsg = msg.LastOrDefault();
                            Akka.Util.Option<long> lastSeq =
                                Akka.Util.Option<long>.None;
                            if (lastMsg != null)
                            {
                                lastSeq =
                                    getSequenceNumber(lastMsg);
                            }

                            var hasLastEvent =
                                lastSeq.HasValue &&
                                lastSeq.Value >= endAt;
                            BatchFlowControl nextControl = null;
                            if (hasLastEvent || opt.Item1 > endAt)
                            {
                                nextControl = BatchFlowControl.Stop.Instance;
                            }
                            else if (hasMoreEvents)
                            {
                                nextControl =
                                    BatchFlowControl.Continue.Instance;
                            }
                            else if (refreshInterval.HasValue == false)
                            {
                                nextControl = BatchFlowControl.Stop.Instance;
                            }
                            else
                            {
                                nextControl = BatchFlowControl.ContinueDelayed
                                    .Instance;
                            }

                            long nextFrom = 0;
                            if (lastSeq.HasValue)
                            {
                                nextFrom = lastSeq.Value + 1;
                            }
                            else
                            {
                                nextFrom = opt.Item1;
                            }

                            return new Akka.Util.Option<((long, BatchFlowControl
                                ),
                                Seq<TElem>)>((
                                (nextFrom, nextControl), msg));
                        }

                        switch (opt.Item2)
                        {
                            case BatchFlowControl.Stop _:
                                return Akka.Util
                                    .Option<((long, BatchFlowControl),
                                        Seq<TElem>)>
                                    .None;
                            case BatchFlowControl.Continue _:
                                return await RetrieveNextBatch();
                            case BatchFlowControl.ContinueDelayed _
                                when refreshInterval.HasValue:
                                return await FutureTimeoutSupport.After(
                                    refreshInterval.Value.Item1,
                                    refreshInterval.Value.Item2,
                                    RetrieveNextBatch);
                            default:
                                throw new Exception(
                                    $"Got invalid BatchFlowControl from Queue! Type : {opt.Item2.GetType()}");
                        }
                    }).SelectMany(r => r);
    }
}