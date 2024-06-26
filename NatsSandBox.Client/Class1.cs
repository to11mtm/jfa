using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Net.Sockets;
using System.Text;
using System.Threading.Channels;
using CommunityToolkit.HighPerformance.Buffers;
using Cysharp.Serialization.MessagePack;
using LanguageExt;
using LinqToDB;
using LinqToDB.Data;
using LinqToDB.Mapping;
using MessagePack;
using MessagePack.Resolvers;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Client.KeyValueStore;
using ValueTaskSupplement;
using static LanguageExt.Prelude;
using CancellationTokenSource = System.Threading.CancellationTokenSource;
using Seq = LanguageExt.Seq;
using Ulid = System.Ulid;

namespace NatsSandBox.Client;

internal class Program
{
    public static async Task Main(string[] args)
    {
        DataConnection.DefaultOnTraceConnection = info =>
        {
            if ((info.Operation == TraceOperation.ExecuteReader
                 || info.Operation == TraceOperation.ExecuteNonQuery)
                && info.TraceInfoStep == TraceInfoStep.BeforeExecute)
            {
                Console.WriteLine(
                    $"{info.Operation} {Environment.NewLine} {info.SqlText}");
            }
        };
        DataConnection.TraceSwitch.Level = TraceLevel.Verbose;
        var memReplConnectionFactory = SqliteInMemDataConnectionFactory.Create();
        using (var ctx = memReplConnectionFactory.GetConnection())
        {
            ctx.CreateTable<ServerReplicationRow>();
            ctx.CreateTable<ServerReplicationSnapshotRow>();
        }
        var cb = new ClientBootstrap();
        await cb.Startclient();
        var c = new TaskSetCoordinatorHashMap("test",cb,false);
        var tcs =
            new TaskCompletionSource(TaskCreationOptions
                .RunContinuationsAsynchronously);
        tcs.SetResult();
        var cts = new CancellationTokenSource();
        var repl = new ServerReplicator(cb, memReplConnectionFactory, new ServerReplicatorOptions(true));
        await repl.StartReplicator(cts.Token);
        await c.Init(tcs,cts.Token);
        await c.CreateTask("idk", "wat", "bar", DateTimeOffset.Now.AddDays(1));

        var t= Task.Run(async () =>
        {
            while (cts.IsCancellationRequested == false)
            {
                try
                {

                    //await c.CreateTask("idk", "wat", "bar", DateTimeOffset.Now.AddDays(1));
                    await Task.Delay(100);
                    //Console.WriteLine(MessagePackSerializer.SerializeToJson(c.GetTaskState(),
                    //    ChatTaskClientSerializer<TaskSetCoordinatorHashMap.TaskState>
                    //        .DefaultSerializerOptions));
                    await Task.Delay(5000);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    throw;
                }
            }
        });
        while (cts.IsCancellationRequested == false)
        {
            var x = Console.ReadLine();
            if (x == "exit")
            {
                cts.Cancel();
            }
            else if (x.StartsWith("create"))
            {
                var tokens = x.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                if (tokens.Length > 3)
                {
                    var type = tokens[1];
                    var assignedTo = tokens[2];
                    var text = string.Join(' ', tokens.Skip(3));
                    var newTaskId = await c.CreateTask(type, text, assignedTo,
                        DateTimeOffset.Now.AddDays(7));
                    Console.WriteLine($"Created Task with ID {newTaskId.ToString()}");
                }
            }
            else if (x.StartsWith("print"))
            {
                var ts = c.GetTaskState();
                foreach (var valueTuplese in Seq.create(
                             ("Assigned : ", ts.AssignedTasks),
                             ("Requested : ", ts.RequestedTasks),
                             ("Unassigned : ", ts.UnassignedTasks))) 
                {
                    Console.WriteLine(valueTuplese.Item1);
                    Console.WriteLine(MessagePackSerializer.SerializeToJson(valueTuplese.Item2,
                        ChatTaskClientSerializer<HashMap<string,HashMap<string,Lst<BaseTaskEvent>>>>
                            .DefaultSerializerOptions));
                    Console.WriteLine("================================");
                }
            }
            else if (x.StartsWith("db printrows"))
            {
                using (var ctx = memReplConnectionFactory.GetConnection())
                {
                    Console.WriteLine((await ctx.GetTable<ServerReplicationRow>()
                        .ToListAsync()).Count);
                }
            }
        }

        await t;
        await Task.WhenAll(repl.ReadLoop, repl.WriteLoop);
        cts.Dispose();
        Console.WriteLine("Goodbye, World!");
    }
}


#region wip stuff

public class JetStreamClientBootstrap
{
    public JetStreamClientBootstrap(NatsOpts? opts = null)
    {
        _opts = opts ?? NatsOpts.Default;
    }
    private NatsConnection _connection;
    private readonly NatsOpts _opts;
    private NatsJSContext _jsCtx;

    public async ValueTask StartClient()
    {
        //TODO: At some point, do jetstream instead.
        //      And after that, do snapshots via KV!
        _connection = new NatsConnection(_opts);
        await _connection.ConnectAsync();
        var jsi = new JetstreamInitiator();
        _jsCtx =  await jsi.EnsureJetstreams(_connection);
        //var js = new NatsJSContext(_connection, new NatsJSOpts(NatsOpts.Default));
        //new NatsKVContext()
    }

    public async ValueTask SendMessage(ISubjectFactory command, CancellationToken token)
    {
        switch (command)
        {
            
            case IChatTaskClient ctc:
                await _jsCtx.PublishAsync(command.GetSubject(),ctc,
                    serializer: ChatTaskClientSerializer<IChatTaskClient>
                        .Default, new NatsJSPubOpts(), cancellationToken: token);
                break;
            case BaseTaskEvent te:
                await _jsCtx.PublishAsync(command.GetSubject(), te,
                    serializer: ChatTaskClientSerializer<BaseTaskEvent>.Default,
                    new NatsJSPubOpts(), cancellationToken: token);
                //await _connection.PublishAsync(command.GetSubject(), te,
                //    serializer: ChatTaskClientSerializer<BaseTaskEvent>.Default);
                break;
        }
    }

    public async ValueTask<INatsSub<T>> SubChat<T>(string subject,
        CancellationToken token) where T : IChatTaskClient
    {
        return await _connection.SubscribeCoreAsync<T>(subject,
            null, ChatTaskClientSerializer<T>.Default, default, token);
    }
    public async ValueTask<IAsyncEnumerable<NatsJSMsg<T>>> Sub<T>(string[] subject, ulong startAt = 0,
        CancellationToken token = default) where T : BaseTaskEvent
    {
        var consumer = await _jsCtx.CreateOrderedConsumerAsync(SubjectHelpers.TaskReplication,
            new NatsJSOrderedConsumerOpts() with { OptStartSeq = startAt, FilterSubjects = subject }, token);
        return consumer.ConsumeAsync(ChatTaskClientSerializer<T>.Default, new NatsJSConsumeOpts(), token);
    }
}

public class JSReadWriteContextPair
{
    public JSReadWriteContextPair(NatsJSContext read, NatsJSContext write)
    {
        Read = read;
        Write = write;
    }
    public NatsJSContext Read { get; }
    public NatsJSContext Write { get; }
}
public class JetstreamInitiator
{
    public async Task<NatsJSContext> EnsureJetstreams(NatsConnection connection)
    {
        var ctx = new NatsJSContext(connection);
        
        var str = await ctx.CreateStreamAsync(new StreamConfig(
                "sample.js.task.events.write",
                new[] { "sample.task.>" }) with
            {
                Discard = StreamConfigDiscard.New,
                Retention = StreamConfigRetention.Interest,
                Storage = StreamConfigStorage.Memory,
                //Republish = new Republish {Src = "sample.task.>", Dest = "sample.task.events.read"}
            });
        var replicaStr = await ctx.CreateStreamAsync(
            new StreamConfig("sample.js.task.events.read",
                //new[] { "sample.task.>" })
                System.Array.Empty<string>())
            {
                Sources = new List<StreamSource>(){new StreamSource(){Name = "sample.js.task.events.write",}},
                Discard = StreamConfigDiscard.New, Retention = StreamConfigRetention.Limits,
                Storage = StreamConfigStorage.Memory, 
            });
        return ctx;
    }
}
#endregion

public class SqliteInMemDataConnectionFactory : DataConnectionFactory
{
    private static int counter = 0;
    private static string genCs()
    {
        return new SqliteConnectionStringBuilder()
        { DataSource = $"memdb-{Interlocked.Increment(ref counter)}",
            Mode = SqliteOpenMode.Memory, Cache = SqliteCacheMode.Shared, Pooling = true
        }.ToString();
    }

    private SqliteInMemDataConnectionFactory()
    {
        _connectionString = genCs();
        Options = new DataOptions(
            new ConnectionOptions(ProviderName: ProviderName.SQLiteMS,
                ConnectionString: _connectionString));
        _heldConnection = new SqliteConnection(_connectionString);
        _heldConnection.Open();
    }

    public readonly DataOptions Options;

    private readonly string _connectionString;
    private readonly SqliteConnection _heldConnection;

    public override DataConnection GetConnection()
    {
        return new DataConnection(Options);
    }

    private static readonly Atom<Lst<SqliteInMemDataConnectionFactory>> _atom =
        Atom(Lst<SqliteInMemDataConnectionFactory>.Empty);
    public static SqliteInMemDataConnectionFactory Create()
    {
        var fact = new SqliteInMemDataConnectionFactory();
        _atom.Swap(fact, (a, b) => b.Add(a));
        return fact;
    }
}
public class ServerReplicationRow
{
    //public string Category { get; set; }
    public string TaskType { get; set; }
    public Guid EventId { get; set; }
    public Guid TaskId { get; set; }
    public byte[] TaskEventData { get; set; }
    public string OriginalSubject { get; set; }
    public ulong? SourceStreamOrdering { get; set; }
    [Column(Configuration = ProviderName.SQLite, DbType = "INTEGER", IsPrimaryKey = true,IsIdentity = true)]
    [Column(IsPrimaryKey = true, IsIdentity = true)]
    public long Ordering { get; set; }
}

public class ServerReplicationSnapshotRow
{
    public long LastDbOrdering { get; set; }
    public byte[] TaskSnapshot { get; set; }
}

public class ReplicationRowHolder
{
    public readonly List<ServerReplicationRow> Rows;
    public readonly TaskCompletionSource<long>? Tcs;

    public ReplicationRowHolder(List<ServerReplicationRow> rows, bool b)
    {
        Rows = rows;
        Tcs = b? new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously) : null;
    }
}

public class ReplicationSqlReader
{
    private readonly DataConnectionFactory _ctxFactory;

    public ReplicationSqlReader(DataConnectionFactory ctxFactory)
    {
        _ctxFactory = ctxFactory;
    }

    public async ValueTask<ServerReplicationSnapshotRow?> GetNewestSnapshot()
    {
        using (var ctx = _ctxFactory.GetConnection())
        {
            var row = await ctx.GetTable<ServerReplicationSnapshotRow>()
                .OrderByDescending(a => a.LastDbOrdering).FirstOrDefaultAsync();
            return row;
        }
    }

    public async IAsyncEnumerable<ServerReplicationRow> BeginReadAtOrdering(long ordering)
    {
        long? curr = ordering;
        List<ServerReplicationRow> currSet = null;
        while (curr != null)
        {
            using (var ctx = _ctxFactory.GetConnection())
            {
                currSet = await GrabOrderingSet(ctx, curr.Value);
                if (currSet.Count < 100)
                {
                    curr = null;
                }
                else
                {
                    curr = currSet.LastOrDefault()!.Ordering + 1;
                }
            }
            foreach (var row in currSet)
            {
                yield return row;
            }
        }
    }

    private static async Task<List<ServerReplicationRow>> GrabOrderingSet(DataConnection ctx, [DisallowNull] long curr)
    {
        return await ctx.GetTable<ServerReplicationRow>().Where(a => a.Ordering >= curr)
            .OrderBy(a=>a.Ordering)
            .Take(100)
            .ToListAsync();
    }
}

public class ReplicatorSqlWriter
{
    private readonly DataConnectionFactory _ctxFactory;

    private static readonly BulkCopyOptions DefaultCopyOptions =
        new BulkCopyOptions(
            BulkCopyType: BulkCopyType.MultipleRows);

    public ReplicatorSqlWriter(DataConnectionFactory dataConnectionFactory)
    {
        _ctxFactory = dataConnectionFactory;
    }

    public async ValueTask<long?> WriteTasks(List<ServerReplicationRow> rows,
        bool includeMaxOrderInReturn)
    {
        try
        {


            using (var ctx = _ctxFactory.GetConnection())
            {
                using (var tx = await ctx.BeginTransactionAsync())
                {
                    long? ret = null;
                    await ctx.GetTable<ServerReplicationRow>()
                        .BulkCopyAsync(DefaultCopyOptions, rows);
                    if (includeMaxOrderInReturn)
                    {
                        var lastEventId = rows.Last().EventId;
                        ret = await ctx.GetTable<ServerReplicationRow>()
                            .Where(a => a.EventId == lastEventId)
                            .Select(a => a.Ordering).OrderByDescending(a => a)
                            .FirstOrDefaultAsync();
                    }

                    await tx.CommitAsync();
                    return ret;
                }
            }
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
    }

    public async ValueTask WriteSnapshot(ServerReplicationSnapshotRow row)
    {
        using (var ctx = _ctxFactory.GetConnection())
        {
            await ctx.GetTable<ServerReplicationSnapshotRow>()
                .InsertOrUpdateAsync(() => new ServerReplicationSnapshotRow()
                    {
                        LastDbOrdering = row.LastDbOrdering,
                        TaskSnapshot = row.TaskSnapshot
                    }, e => new ServerReplicationSnapshotRow()
                    {
                        TaskSnapshot = row.TaskSnapshot
                    },
                    () => new ServerReplicationSnapshotRow()
                        { LastDbOrdering = row.LastDbOrdering });
        }
    }
}

public abstract class ClientProvider
{
    public abstract ValueTask EnsureStarted(CancellationToken token = default);
    public abstract ValueTask SendMessage<T>(T taskRequest, CancellationToken token = default) where T:BaseTaskEvent;
    public abstract IAsyncEnumerable<NatsBaseEvent<T>> GetConsumer<T>(string subject,ulong startAt = 0, CancellationToken token = default) where T : BaseTaskEvent;
}

public sealed class StandardClientProvider : ClientProvider
{
    private readonly ClientBootstrap _bootstrap;

    public StandardClientProvider(ClientBootstrap bootstrap)
    {
        _bootstrap = bootstrap;
    }
    public override async ValueTask EnsureStarted(CancellationToken token = default)
    {
        await _bootstrap.Startclient();
    }

    public override async ValueTask SendMessage<T>(T taskRequest, CancellationToken token = default)
    {
        await _bootstrap.SendMessage(taskRequest, token);
    }

    public override async IAsyncEnumerable<NatsBaseEvent<T>> GetConsumer<T>(string subject, ulong startAt = 0, CancellationToken token = default)
    {
        await foreach(var item in (await _bootstrap.Sub<T>( subject,token)).Msgs.ReadAllAsync(token))
        {
            yield return new NatsEvent<T>(item);
        }
    }
}
public sealed class JetStreamClientProvider : ClientProvider
{
    private readonly JetStreamClientBootstrap _bootstrap;

    public JetStreamClientProvider(JetStreamClientBootstrap bootstrap)
    {
        _bootstrap = bootstrap;
    }
    public override async ValueTask EnsureStarted(CancellationToken token = default)
    {
        await _bootstrap.StartClient();
    }

    public override async ValueTask SendMessage<T>(T taskRequest, CancellationToken token = default)
    {
        await _bootstrap.SendMessage(taskRequest, token);
    }

    public override async IAsyncEnumerable<NatsBaseEvent<T>> GetConsumer<T>(string subject, ulong startAt = 0, CancellationToken token = default)
    {
        await foreach(var item in ((await _bootstrap.Sub<T>(new[] { subject }, startAt, token)).WithCancellation(token)))
        {
            yield return new NatsBaseJsEvent<T>(item);
        }
    }
}
public record ServerReplicatorOptions(bool ForceWriteDispatchViaTaskScheduler);
public class ServerReplicatorV2
{
    private readonly ClientProvider _client;
    private readonly DataConnectionFactory _ctxFactory;
    
    public ServerReplicatorV2(ClientProvider bootstrap,
        DataConnectionFactory dataConnectionFactory, ServerReplicatorOptions options)
    {
        _client = bootstrap;
        _replicationSqlReader = new ReplicationSqlReader(dataConnectionFactory);
        _replicatorSqlWriter = new ReplicatorSqlWriter(dataConnectionFactory);
        _options = options;
    }


    private readonly Channel<ReplicationRowHolder> _serverRows =
        Channel.CreateUnbounded<ReplicationRowHolder>();

    private readonly ReplicationSqlReader _replicationSqlReader;

    public async ValueTask StartReplicator(CancellationToken token)
    {
        var wStart =
            new TaskCompletionSource(TaskCreationOptions
                .RunContinuationsAsynchronously);
        var rStart =
            new TaskCompletionSource(TaskCreationOptions
                .RunContinuationsAsynchronously);
        WriteLoop = Task.Run(async () =>
        {
            try
            {
                Task lastWrite = Task.CompletedTask;

                // TODO: Pooling
                List<ServerReplicationRow> pendingSet =
                    new List<ServerReplicationRow>();
                List<TaskCompletionSource<long>> pendingRange =
                    new List<TaskCompletionSource<long>>();
                
                // ReSharper disable once MethodSupportsCancellation
                wStart.SetResult();
                
                async Task CycleFlush(List<ServerReplicationRow> serverReplicationRows, List<TaskCompletionSource<long>> taskCompletionSources)
                {
                    await lastWrite;
                    if (_options.ForceWriteDispatchViaTaskScheduler)
                    {
                        lastWrite =
                            Task.Run(async () =>
                            {
                                await FlushMainWriteBufferAndClearPendings(
                                    serverReplicationRows,
                                    taskCompletionSources);
                            });
                    }
                    else
                    {
                        lastWrite =
                            FlushMainWriteBufferAndClearPendings(serverReplicationRows,
                                taskCompletionSources);
                    }

                    pendingSet = new List<ServerReplicationRow>();
                    pendingRange = new List<TaskCompletionSource<long>>();
                }

                while (await _serverRows.Reader.WaitToReadAsync())
                {
                    while (_serverRows.Reader.TryRead(out var a))
                    {
                        pendingSet.AddRange(a.Rows);
                        if (a.Tcs != null)
                        {
                            pendingRange.Add(a.Tcs);
                        }

                        //If close to a flush, cycle.
                        if (maxTillMainFlush - pendingSet.Count - maxTillFlush <= 0)
                        {
                            await CycleFlush(pendingSet, pendingRange);
                        }
                    }
                    // ensure last batch is cycled, just in case it's a while
                    // until the next event.
                    if (pendingSet.Count > 0)
                    {
                        await CycleFlush(pendingSet, pendingRange);
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        });
        ReadLoop = Task.Run(async () =>
        {
            try
            {
                // TODO: SetResult/SetException based on sub call.
                rStart.SetResult();
                var passingCh = Channel.CreateBounded<NatsBaseEvent<BaseTaskEvent>>(8);
                var snapImpl = new ReplicatorSnapshotTracker();
                var lastRead = await RecoverImpl(snapImpl);
                Task.Run(async () =>
                {
                    var w = passingCh.Writer;
                    await foreach (var item in _client.GetConsumer<BaseTaskEvent>(SubjectHelpers.TaskReplication, lastRead.GetValueOrDefault(0), token))
                    {
                        await w.WriteAsync(item, token);
                    }
                });
                try
                {
                    var r = passingCh.Reader;
                    
                    var tillSnap = maxTillSnap;
                    var thisList = new List<ServerReplicationRow>();
                    ReplicationRowHolder holder = null;
                    while (token.IsCancellationRequested == false)
                    {
                        var tillFlush = maxTillFlush;
                        NatsBaseEvent<BaseTaskEvent>? msg = default;
                        while (r.TryRead(out msg))
                        {
                            await snapImpl.ConsumeEvent(msg.Data!);
                            thisList.Add(ToReplicationRow(msg));

                            tillFlush--;
                            if (tillFlush == 0)
                            {
                                tillSnap =
                                    await FlushAndSnapBarrier(token, snapImpl,
                                        tillSnap, thisList, msg);
                                thisList = new List<ServerReplicationRow>();
                            }
                        }

                        if (tillFlush != maxTillFlush)
                        {
                            tillSnap = await FlushAndSnapBarrier(
                                token,
                                snapImpl, tillSnap, thisList, msg);
                            thisList = new List<ServerReplicationRow>();
                        }

                        if (await r.WaitToReadAsync(token) == false)
                        {
                            break;
                        }
                    }

                    if (thisList.Count > 0)
                    {
                        await _serverRows.Writer.WriteAsync(
                            new ReplicationRowHolder(thisList, false), token);
                        //this is shutdown, no need to realloc list here.
                    }
                }
                finally
                {
                    passingCh.Writer.TryComplete();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        });
        await Task.WhenAll(wStart.Task, rStart.Task);
    }

    private async Task FlushMainWriteBufferAndClearPendings(
        List<ServerReplicationRow> pendingSet,
        List<TaskCompletionSource<long>> pendingRange)
    {
        var r = await _replicatorSqlWriter.WriteTasks(pendingSet, true);
        if (r != null)
        {
            foreach (var taskCompletionSource in pendingRange)
            {
                taskCompletionSource.TrySetResult(r.Value);
            }
        }
    }

    const int maxTillFlush = 100;
    const int maxTillSnap = 2000;
    private const int maxTillMainFlush = 500;
    private async Task<int> FlushAndSnapBarrier(CancellationToken token,
        ReplicatorSnapshotTracker snapImpl,
        int tillSnap, List<ServerReplicationRow> thisList, NatsBaseEvent<BaseTaskEvent>? msg)
    {
        tillSnap = tillSnap - thisList.Count;
        var shouldSnap = (tillSnap <= 0);
        ReplicationRowHolder holder =
            holder = new ReplicationRowHolder(thisList, shouldSnap);
        await _serverRows.Writer.WriteAsync(holder, token);
        if (shouldSnap)
        {
            var replRow = snapImpl.GetSnapState();
            _ = Task.Run(async () =>
            {
                await _replicatorSqlWriter.WriteSnapshot(
                    new ServerReplicationSnapshotRow()
                    {
                        LastDbOrdering = await holder.Tcs.Task,
                        TaskSnapshot =
                            ReplicationSerializer.Instance
                                .SerializeTaskSnapshot(replRow)
                    });
                tillSnap = maxTillSnap;
            }, token);
            tillSnap = maxTillSnap;
        }

        if (msg != null)
        {
            await msg.AckAsync(token);
        }
        return tillSnap;
    }

    private static void ThrowClosedChannelException()
    {
        throw new ChannelClosedException("Row Buffer Channel was closed!");
    }

    private ServerReplicationRow ToReplicationRow(NatsBaseEvent<BaseTaskEvent> item)
    {
        var msg = item.Data;
        var tt = msg.TaskType;
        var eid = msg.EventId;
        var tid = msg.TaskId;
        byte[] bytes = default;
        using (var buf = new ArrayPoolBufferWriter<byte>())
        {
            ChatTaskClientSerializer<BaseTaskEvent>.Default.Serialize(buf, msg);
            bytes = buf.WrittenMemory.ToArray();
        }
        
        return new ServerReplicationRow()
        {
            SourceStreamOrdering = item.Sequence,
            EventId = eid.ToGuid(), OriginalSubject = item.Subject,
            TaskEventData = bytes, TaskId = tid.ToGuid(), TaskType = tt
        };
    }

    private readonly ReplicatorSqlWriter _replicatorSqlWriter;
    private readonly ServerReplicatorOptions _options;
    public Task WriteLoop { get; private set; } = Task.CompletedTask;
    public Task ReadLoop { get; private set; } = Task.CompletedTask;

    private async ValueTask<ulong?> RecoverImpl(ReplicatorSnapshotTracker consumer)
    {
        var s = await _replicationSqlReader.GetNewestSnapshot();
        if (s != null)
        {
            await consumer.ConsumeSnap(
                ReplicationSerializer.Instance.DeSerializeTaskSnapshot(
                    s!.TaskSnapshot));
        }

        var o = s?.LastDbOrdering ?? 0;
        ulong? lastRead = default; 
        await foreach (var a in _replicationSqlReader.BeginReadAtOrdering(o))
        {
            await consumer.ConsumeEvent(
                ReplicationSerializer.Instance.GetTaskEvent(a.TaskEventData));
            lastRead = a.SourceStreamOrdering;
        }

        return lastRead;
    }
}
public class ServerReplicator
{
    private readonly ClientBootstrap _client;
    private readonly DataConnectionFactory _ctxFactory;
    
    public ServerReplicator(ClientBootstrap bootstrap,
        DataConnectionFactory dataConnectionFactory, ServerReplicatorOptions options)
    {
        _client = bootstrap;
        _replicationSqlReader = new ReplicationSqlReader(dataConnectionFactory);
        _replicatorSqlWriter = new ReplicatorSqlWriter(dataConnectionFactory);
        _options = options;
    }


    private readonly Channel<ReplicationRowHolder> _serverRows =
        Channel.CreateUnbounded<ReplicationRowHolder>();

    private readonly ReplicationSqlReader _replicationSqlReader;

    public async ValueTask StartReplicator(CancellationToken token)
    {
        var wStart =
            new TaskCompletionSource(TaskCreationOptions
                .RunContinuationsAsynchronously);
        var rStart =
            new TaskCompletionSource(TaskCreationOptions
                .RunContinuationsAsynchronously);
        WriteLoop = Task.Run(async () =>
        {
            try
            {
                Task lastWrite = Task.CompletedTask;

                // TODO: Pooling
                List<ServerReplicationRow> pendingSet =
                    new List<ServerReplicationRow>();
                List<TaskCompletionSource<long>> pendingRange =
                    new List<TaskCompletionSource<long>>();
                
                // ReSharper disable once MethodSupportsCancellation
                wStart.SetResult();
                
                async Task CycleFlush(List<ServerReplicationRow> serverReplicationRows, List<TaskCompletionSource<long>> taskCompletionSources)
                {
                    await lastWrite;
                    if (_options.ForceWriteDispatchViaTaskScheduler)
                    {
                        lastWrite =
                            Task.Run(async () =>
                            {
                                await FlushMainWriteBufferAndClearPendings(
                                    serverReplicationRows,
                                    taskCompletionSources);
                            });
                    }
                    else
                    {
                        lastWrite =
                            FlushMainWriteBufferAndClearPendings(serverReplicationRows,
                                taskCompletionSources);
                    }

                    pendingSet = new List<ServerReplicationRow>();
                    pendingRange = new List<TaskCompletionSource<long>>();
                }

                while (await _serverRows.Reader.WaitToReadAsync())
                {
                    while (_serverRows.Reader.TryRead(out var a))
                    {
                        pendingSet.AddRange(a.Rows);
                        if (a.Tcs != null)
                        {
                            pendingRange.Add(a.Tcs);
                        }

                        //If close to a flush, cycle.
                        if (maxTillSnap - pendingSet.Count - maxTillFlush <= 0)
                        {
                            await CycleFlush(pendingSet, pendingRange);
                        }
                    }
                    // ensure last batch is cycled, just in case it's a while
                    // until the next event.
                    if (pendingSet.Count > 0)
                    {
                        await CycleFlush(pendingSet, pendingRange);
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        });
        ReadLoop = Task.Run(async () =>
        {
            try
            {
                // TODO: SetResult/SetException based on sub call.
                rStart.SetResult();
                await using (var sub =
                             await _client.Sub<BaseTaskEvent>(
                                 SubjectHelpers.TaskReplication,
                                 token))
                {
                    var snapImpl = new ReplicatorSnapshotTracker();
                    await RecoverImpl(snapImpl);
                    var tillSnap = maxTillSnap;
                    var thisList = new List<ServerReplicationRow>();
                    ReplicationRowHolder holder = null;
                    while (token.IsCancellationRequested == false)
                    {
                        var tillFlush = maxTillFlush;
                        while (sub.Msgs.TryRead(out var msg))
                        {
                            await snapImpl.ConsumeEvent(msg.Data!);
                            thisList.Add(ToReplicationRow(msg));

                            tillFlush--;
                            if (tillFlush == 0)
                            {
                                tillSnap =
                                    await FlushAndSnapBarrier(token, snapImpl,
                                        tillSnap, thisList);
                                thisList =  new List<ServerReplicationRow>();
                            }
                        }

                        if (tillFlush != maxTillFlush)
                        {
                            tillSnap = await FlushAndSnapBarrier(
                                token,
                                snapImpl, tillSnap, thisList);
                            thisList = new List<ServerReplicationRow>();
                        }

                        if (await sub.Msgs.WaitToReadAsync(token) == false)
                        {
                            break;
                        }
                    }

                    if (thisList.Count > 0)
                    {
                        await _serverRows.Writer.WriteAsync(
                            new ReplicationRowHolder(thisList, false), token);
                        //this is shutdown, no need to realloc list here.
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        });
        await Task.WhenAll(wStart.Task, rStart.Task);
    }

    private async Task FlushMainWriteBufferAndClearPendings(
        List<ServerReplicationRow> pendingSet,
        List<TaskCompletionSource<long>> pendingRange)
    {
        var r = await _replicatorSqlWriter.WriteTasks(pendingSet, true);
        if (r != null)
        {
            foreach (var taskCompletionSource in pendingRange)
            {
                taskCompletionSource.TrySetResult(r.Value);
            }
        }
    }

    const int maxTillFlush = 100;
    const int maxTillSnap = 2000;
    private const int maxTillMainFlush = 500;
    private async Task<int>
        FlushAndSnapBarrier(CancellationToken token,
            ReplicatorSnapshotTracker snapImpl,
            int tillSnap, List<ServerReplicationRow> thisList)
    {
        tillSnap = tillSnap - thisList.Count;
        var shouldSnap = (tillSnap <= 0);
        ReplicationRowHolder holder =
            holder = new ReplicationRowHolder(thisList, shouldSnap);
        await _serverRows.Writer.WriteAsync(holder, token);
        if (shouldSnap)
        {
            var replRow = snapImpl.GetSnapState();
            _ = Task.Run(async () =>
            {
                await _replicatorSqlWriter.WriteSnapshot(
                    new ServerReplicationSnapshotRow()
                    {
                        LastDbOrdering = await holder.Tcs.Task,
                        TaskSnapshot =
                            ReplicationSerializer.Instance
                                .SerializeTaskSnapshot(replRow)
                    });
                tillSnap = maxTillSnap;
            }, token);
            tillSnap = maxTillSnap;
        }

        return tillSnap;
    }

    private static void ThrowClosedChannelException()
    {
        throw new ChannelClosedException("Row Buffer Channel was closed!");
    }

    private ServerReplicationRow ToReplicationRow(NatsMsg<BaseTaskEvent> item)
    {
        var msg = item.Data;
        var tt = msg.TaskType;
        var eid = msg.EventId;
        var tid = msg.TaskId;
        byte[] bytes = default;
        using (var buf = new ArrayPoolBufferWriter<byte>())
        {
            ChatTaskClientSerializer<BaseTaskEvent>.Default.Serialize(buf, msg);
            bytes = buf.WrittenMemory.ToArray();
        }

        return new ServerReplicationRow()
        {
            //Ordering gets filled in by SQL, for now...
            EventId = eid.ToGuid(), OriginalSubject = item.Subject,
            TaskEventData = bytes, TaskId = tid.ToGuid(), TaskType = tt
        };
    }

    private readonly ReplicatorSqlWriter _replicatorSqlWriter;
    private readonly ServerReplicatorOptions _options;
    public Task WriteLoop { get; private set; } = Task.CompletedTask;
    public Task ReadLoop { get; private set; } = Task.CompletedTask;

    private async ValueTask RecoverImpl(ReplicatorSnapshotTracker consumer)
    {
        var s = await _replicationSqlReader.GetNewestSnapshot();
        if (s != null)
        {
            await consumer.ConsumeSnap(
                ReplicationSerializer.Instance.DeSerializeTaskSnapshot(
                    s!.TaskSnapshot));
        }

        var o = s?.LastDbOrdering ?? 0;
        await foreach (var a in _replicationSqlReader.BeginReadAtOrdering(o))
        {
            await consumer.ConsumeEvent(
                ReplicationSerializer.Instance.GetTaskEvent(a.TaskEventData));
        }
    }
}

public abstract record NatsBaseEvent<T>
{
    public abstract T? Data { get; }
    public abstract string Subject { get; }
    public abstract NatsHeaders? NatsHeaders { get; }
    public abstract ulong? Sequence { get; }
    public abstract ValueTask AckAsync(CancellationToken token = default);
    public abstract ValueTask NackAsync(CancellationToken token = default);
}

public record NatsEvent<T>(NatsMsg<T> Msg) : NatsBaseEvent<T>
{
    public override T? Data => Msg.Data;
    public override string Subject => Msg.Subject;
    public override NatsHeaders? NatsHeaders => Msg.Headers;
    public override ulong? Sequence => null;

    public override ValueTask AckAsync(CancellationToken token = default)
    {
        return ValueTask.CompletedTask;
    }

    public override ValueTask NackAsync(CancellationToken token = default)
    {
        return ValueTask.CompletedTask;
    }
}
public record NatsBaseJsEvent<T>(NatsJSMsg<T> Msg) : NatsBaseEvent<T>
{
    public override T? Data => Msg.Data;
    public override string Subject => Msg.Subject;
    public override NatsHeaders? NatsHeaders => Msg.Headers;
    public override ulong? Sequence => Msg.Metadata != null? Msg.Metadata.Value.Sequence.Stream:null;
    public override async ValueTask AckAsync(CancellationToken token = default)
    {
        await Msg.AckAsync(cancellationToken: token);
    }

    public override async ValueTask NackAsync(CancellationToken token = default)
    {
        await Msg.NakAsync(cancellationToken: token);
    }
}

public class ReplicationReader
{
    
}
public class ReplicationSerializer
{
    public static readonly ReplicationSerializer Instance =
        new ReplicationSerializer();

    public BaseTaskEvent GetTaskEvent(byte[] bytes)
    {
        return ChatTaskClientSerializer<BaseTaskEvent>.Default.Deserialize(
            new ReadOnlySequence<byte>(bytes))!;
    }

    public byte[] SerializeTaskEvent(BaseTaskEvent taskEvt)
    {
        using (var apbw = new ArrayPoolBufferWriter<byte>())
        {
            ChatTaskClientSerializer<BaseTaskEvent>.Default.Serialize(apbw,
                taskEvt);
            return apbw.WrittenMemory.ToArray();
        }
    }
    
    public TaskSerializationState DeSerializeTaskSnapshot(byte[] bytes)
    {
        return ChatTaskClientSerializer<TaskSerializationState>.Default.Deserialize(
            new ReadOnlySequence<byte>(bytes))!;
    }

    public byte[] SerializeTaskSnapshot(TaskSerializationState snapState)
    {
        using (var apbw = new ArrayPoolBufferWriter<byte>())
        {
            ChatTaskClientSerializer<TaskSerializationState>.Default.Serialize(apbw,
                snapState);
            return apbw.WrittenMemory.ToArray();
        }
    }
}

public record TaskSerializationState(
    HashMap<string, HashMap<Ulid, Lst<BaseTaskEvent>>> Assigned,
    HashMap<string, HashMap<Ulid, Lst<BaseTaskEvent>>> Unassigned);
public interface ISnapshotAndEventConsumer
{
    IAsyncEnumerable<BaseTaskEvent> BeginNewEventConsumer(
        CancellationToken token = default);
    ValueTask ConsumeSnap(TaskSerializationState row);
    ValueTask ConsumeEvent(BaseTaskEvent row);
}

public abstract class DataConnectionFactory
{
    public abstract DataConnection GetConnection();
}

public class ClientBootstrap
{
    public ClientBootstrap(NatsOpts opts = null)
    {
        _opts = opts ?? NatsOpts.Default;
    }
    private NatsConnection _connection;
    private readonly NatsOpts _opts;

    public async ValueTask Startclient()
    {
        //TODO: At some point, do jetstream instead.
        //      And after that, do snapshots via KV!
        _connection = new NatsConnection(_opts);
        await _connection.ConnectAsync();
        //var js = new NatsJSContext(_connection, new NatsJSOpts(NatsOpts.Default));
        //new NatsKVContext()
    }

    public async ValueTask SendMessage(ISubjectFactory command, CancellationToken token = default)
    {
        switch (command)
        {
            case IChatTaskClient ctc:
                await _connection.PublishAsync(command.GetSubject(), ctc,
                    serializer: ChatTaskClientSerializer<IChatTaskClient>
                        .Default, cancellationToken: token);
                break;
            case BaseTaskEvent te:
                await _connection.PublishAsync(command.GetSubject(), te,
                    serializer: ChatTaskClientSerializer<BaseTaskEvent>.Default, cancellationToken: token);
                break;
        }
    }

    public async ValueTask<INatsSub<T>> SubChat<T>(string subject,
        CancellationToken token) where T : IChatTaskClient
    {
        return await _connection.SubscribeCoreAsync<T>(subject,
            null, ChatTaskClientSerializer<T>.Default, default, token);
    }
    public async ValueTask<INatsSub<T>> Sub<T>(string subject,
        CancellationToken token) where T : BaseTaskEvent
    {
        return await _connection.SubscribeCoreAsync<T>(subject,
            null, ChatTaskClientSerializer<T>.Default, default, token);
    }
}

public record ChatHistory(Lst<BroadcastMessage> Messages)
{
    public ChatHistory AddMsg(BroadcastMessage message)
    {
        if (Messages.Contains(message))
        {
            return this;
        }

        return this with { Messages = Messages.Add(message) };
    }
}

public record ReplicatorStateProjectionData(HashMap<Ulid, Lst<BaseTaskEvent>> Map);

public record ReplicatorStateProjectionSet(
    ReplicatorStateProjectionData Active);
public class ReplicatorSnapshotTracker
{
    private HashMap<string, HashMap<Ulid, Lst<BaseTaskEvent>>> _tasks;
    private HashMap<string, HashMap<Ulid, Lst<BaseTaskEvent>>> _assigned;
    private HashMap<string, HashMap<Ulid, Lst<BaseTaskEvent>>> _unassigned;
    public async ValueTask ConsumeSnap(TaskSerializationState row)
    {
        _tasks = row.Assigned.Union(row.Unassigned, (a, b) => b, (a, b) => b,
            (k, a, b) => a.Union(b));
    }

    public async ValueTask ConsumeEvent(BaseTaskEvent row)
    {
        if (row is TaskCompleted)
        {
            RemoveFromSets();
        }
        else if (row is TaskCanceled)
        {
            RemoveFromSets();
        }
        else
        {
            _tasks =
                _tasks.TrySetItem(row.TaskType,
                    (s) => s.TrySetItem(row.TaskId, a => a.Add(row)));
            _assigned = _assigned.TrySetItem(row.TaskType,
                (s) => s.TrySetItem(row.TaskId, a => a.Add(row)));
            _unassigned = _unassigned.TrySetItem(row.TaskType,
                (s) => s.TrySetItem(row.TaskId, a => a.Add(row)));
        }

        void RemoveFromSets()
        {
            _tasks =
                _tasks.TrySetItem(row.TaskType, (s) => s.Remove(row.TaskId));
            _assigned = _assigned.TrySetItem(row.TaskType, (s) => s.Remove(row.TaskId));
            _unassigned =
                _unassigned.TrySetItem(row.TaskType, (s) => s.Remove(row.TaskId));
        }
    }

    public TaskSerializationState GetSnapState()
    {
        return new TaskSerializationState(_assigned, _unassigned);
    }
}

/// <summary>
/// 
/// </summary>
/// <remarks>
/// Ok. So the way this works, is we use a series of <see cref="Atom{A}"/>
/// Hashmaps of Maps. Atom lets us Swap via interlocked,
/// such that if the values being swapped are 'comparable' and the actions are
/// 'safely repeatable', it can repeat the action if a concurrent update happens
/// until the interlocked operation succeeds.
/// </remarks>
public class TaskSetCoordinatorHashMapJetStream : ISnapshotAndEventConsumer
{
    public string UserId { get; }

    public TaskSetCoordinatorHashMapJetStream(string userId,
        ClientProvider clientBootstrap, bool trackChanges)
    {
        UserId = userId;
        _client = clientBootstrap;
        _trackChanges = trackChanges;
        _assignedTasks = Atom(HashMap<string, HashMap<Ulid, Lst<BaseTaskEvent>>>());
        _requestedTasks =  Atom(HashMap<string, HashMap<Ulid, Lst<BaseTaskEvent>>>());
        _unassignedTasks = Atom(HashMap<string, HashMap<Ulid, Lst<BaseTaskEvent>>>());
    }
    
    private readonly Atom<HashMap<string, HashMap<Ulid, Lst<BaseTaskEvent>>>> _assignedTasks;

    private readonly Atom<HashMap<string, HashMap<Ulid, Lst<BaseTaskEvent>>>> _requestedTasks;

    private readonly Atom<HashMap<string, HashMap<Ulid, Lst<BaseTaskEvent>>>> _unassignedTasks;

    private readonly ClientProvider _client;
    private Task _mainTask;

    public async Task<Ulid> CreateTask(string taskType, string text,
        string? assignedToUser, DateTimeOffset expectedBy, CancellationToken token = default)
    {
        var taskId = Ulid.NewUlid();
        await _client.SendMessage(new TaskRequest(Ulid.NewUlid(), UserId, assignedToUser,
            taskId, taskType, text, DateTimeOffset.UtcNow,
            expectedBy), token);
        return taskId;
    }

    public async ValueTask AddTaskNote(TaskRequest origReq,
        string notes)
    {
        await _client.SendMessage(new TaskUpdate(Ulid.NewUlid(),origReq.TaskId,
            origReq.TaskType, UserId, notes));
    }

    public async ValueTask<bool> AddTaskCanceled(TaskRequest origReq, string notes)
    {
        if (origReq.RequestingUserId == UserId)
        {
            await _client.SendMessage(new TaskCanceled(Ulid.NewUlid(),origReq.TaskId,
                origReq.TaskType, UserId, notes, DateTimeOffset.Now));
            return true;
        }

        return false;
    }

    public async ValueTask<ReassignResult> ReassignTask(Ulid taskId,
        string newUser, string notes)
    {
        bool foundTask = false;
        foreach (var requestedTask in _assignedTasks.Value)
        {
            var item =
                requestedTask.Value.Find(taskId, a => a,
                    static () => Lst<BaseTaskEvent>.Empty);
            if (item != default)
            {
                foundTask = true;
                await _client.SendMessage(new TaskAssigned(Ulid.NewUlid(),taskId, newUser,
                    requestedTask.Key, UserId, notes, item,
                    DateTimeOffset.Now));
                //_assignedTasks.AddOrUpdate(requestedTask.Key,
                //    e => e.Remove(taskId),
                //    () => HashMap<Ulid, Lst<BaseTaskEvent>>.Empty);
                return new ReassignResult(taskId, true, "Reassigned");
            }
        }

        foreach (var requestedTask in _unassignedTasks.Value)
        {
            var item =
                requestedTask.Value.Find(taskId, a => a,
                    static () => Lst<BaseTaskEvent>.Empty);
            if (item != default)
            {
                foundTask = true;
                await _client.SendMessage(new TaskAssigned(Ulid.NewUlid(),taskId, newUser,
                    requestedTask.Key, UserId, notes, item,
                    DateTimeOffset.Now));
                //_assignedTasks.AddOrUpdate(requestedTask.Key,
                //    e => e.Remove(taskId),
                //    () => HashMap<Ulid, Lst<BaseTaskEvent>>.Empty);
                return new ReassignResult(taskId, true, "Reassigned");
            }
        }

        return new ReassignResult(taskId, false, "Could not find task!");
    }

    public async ValueTask Init(TaskCompletionSource initLock,
        CancellationToken token)
    {
        ;
        CancellationTokenSource cts = null;
        _mainTask = Task.Run(async () =>
        {
            await initLock.Task;
            try
            {
                await foreach (var entry in
                    (_client.GetConsumer<BaseTaskEvent>(
                        SubjectHelpers.TaskUserSub(UserId), token: token)).WithCancellation(cts.Token))
                {
                    try
                    {
                        await RunEventHandlers(entry);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                        throw;
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
            finally
            {
                try
                {
                    cts?.Cancel(false);
                }
                finally
                {
                    try
                    {
                        cts?.Dispose();
                    }
                    catch
                    {
                        // ignored
                    }
                }
            }
        });
    }

    private async ValueTask RunEventHandlers(NatsBaseEvent<BaseTaskEvent> entry)
    {
            ProcessTaskEventSwitch(entry.Data);
            await entry.AckAsync();
    }

    private void ProcessTaskEventSwitch(BaseTaskEvent? msg)
    {
        switch (msg)
        {
            case TaskAssigned ta:
            {
                HandleTaskAssigned(ta);

                break;
            }
            case TaskBegan tb:
            {
                HandleTaskBegan(tb);
                break;
            }
            case TaskCompleted tc:
            {
                HandleTaskCompleted(tc);
                break;
            }
            case TaskRequest tr:
            {
                HandleTaskRequest(tr);
                break;
            }
            case TaskUpdate tu:
            {
                HandleTaskUpdate(tu);
                break;
            }
            case TaskCanceled tcan:
            {
                HandleTaskCanceled(tcan);
                break;
            }

        }
    }

    public async ValueTask<ClearResult> ClearItem(Ulid taskId)
    {
        bool? foundAndCleared = null;
        _requestedTasks.Swap(thm =>
        {
            var innerThm = thm;
            foreach (var valueTuple in thm)
            {
                valueTuple.Value.Find(taskId, s =>
                {
                    var f = s.Head();
                    var c = s.Reverse().FirstOrDefault();
                    if (c is TaskCanceled or TaskCompleted ||
                        f is not TaskRequest)
                    {
                        innerThm =  innerThm.AddOrUpdate(valueTuple.Key,
                            thmi => thmi.Remove(taskId),
                            () => LanguageExt.HashMap<Ulid, Lst<BaseTaskEvent>>.Empty);
                    }

                    return true;
                }, () => false);
            }

            return innerThm;
        });
        _requestedTasks.Swap(thm =>
        {
            var innerThm = thm;
            foreach (var valueTuple in thm)
            {
                valueTuple.Value.Find(taskId, s =>
                {
                    var f = s.Head();
                    var c = s.Reverse().FirstOrDefault();
                    if (c is TaskCanceled or TaskCompleted ||
                        f is not TaskRequest)
                    {
                        innerThm =  innerThm.AddOrUpdate(valueTuple.Key,
                            thmi => thmi.Remove(taskId),
                            () => LanguageExt.HashMap<Ulid, Lst<BaseTaskEvent>>.Empty);
                    }

                    return true;
                }, () => false);
            }

            return innerThm;
        });
        _unassignedTasks.Swap(thm =>
        {
            var innerThm = thm;
            foreach (var valueTuple in thm)
            {
                valueTuple.Value.Find(taskId, s =>
                {
                    var f = s.Head();
                    var c = s.Reverse().FirstOrDefault();
                    if (c is TaskCanceled or TaskCompleted ||
                        f is not TaskRequest)
                    {
                        innerThm =  innerThm.AddOrUpdate(valueTuple.Key,
                            thmi => thmi.Remove(taskId),
                            () => LanguageExt.HashMap<Ulid, Lst<BaseTaskEvent>>.Empty);
                    }

                    return true;
                }, () => false);
            }

            return innerThm;
        });
        if (foundAndCleared.HasValue == false)
        {
            return new ClearResult(true, "no instances found");
        }
        else
        {
            if (foundAndCleared.Value)
            {
                return new ClearResult(true, "cleared");
            }
            else
            {
                return new ClearResult(false, "Tasks not in closed state");
            }
        }
    }

    private void HandleTaskRequest(TaskRequest trb)
    {
        void doSwap(
            Atom<HashMap<string,
                HashMap<Ulid, Lst<BaseTaskEvent>>>> taskSet,
            TaskRequest taskRequest)
        {
            taskSet.Swap(taskRequest, (tr,b)=>b.AddOrUpdate(tr.TaskType,
                (hm) => hm.TryAdd(tr.TaskId, Lst<BaseTaskEvent>.Empty.Add(tr)),
                () => LanguageExt.HashMap<Ulid, Lst<BaseTaskEvent>>.Empty.Add(tr.TaskId,
                    Lst<BaseTaskEvent>.Empty.Add(tr))));
        }

        if (trb.RequestingUserId == UserId)
        {
            doSwap(_requestedTasks, trb);
        }

        if (trb.AssignedUserId == UserId)
        {
            doSwap(_assignedTasks,trb);
        }
        else if (trb.AssignedUserId == "unassigned")
        {
            doSwap(_unassignedTasks,trb);
        }
    }

    private static void TUSwap(
        Atom<HashMap<string, HashMap<Ulid, Lst<BaseTaskEvent>>>>
            atom, TaskUpdate tu)
    {
        atom.Value.Find(tu.TaskType, hm =>
        {
            atom.Swap(b=>b.AddOrUpdate(tu.TaskType,
                (e) => e.AddOrUpdate(tu.TaskId, (s) => s.Add(tu),
                    () => Lst<BaseTaskEvent>.Empty.Add(tu)),
                () => LanguageExt.HashMap<Ulid, Lst<BaseTaskEvent>>.Empty));
            return Unit.Default;
        }, () => Unit.Default);
    }
    private static void TCanSwap(
        Atom<HashMap<string, HashMap<Ulid, Lst<BaseTaskEvent>>>>
            atom, TaskCanceled tu)
    {
        atom.Value.Find(tu.TaskType, hm =>
        {
            atom.Swap(b=>b.AddOrUpdate(tu.TaskType,
                (e) => e.AddOrUpdate(tu.TaskId, (s) => s.Add(tu),
                    () => Lst<BaseTaskEvent>.Empty.Add(tu)),
                () => LanguageExt.HashMap<Ulid, Lst<BaseTaskEvent>>.Empty));
            return Unit.Default;
        }, () => Unit.Default);
    }
    private static void TComSwap(
        Atom<HashMap<string, HashMap<Ulid, Lst<BaseTaskEvent>>>>
            atom, TaskCompleted tu)
    {
        atom.Value.Find(tu.TaskType, hm =>
        {
            atom.Swap(b=>b.AddOrUpdate(tu.TaskType,
                (e) => e.AddOrUpdate(tu.TaskId, (s) => s.Add(tu),
                    () => Lst<BaseTaskEvent>.Empty.Add(tu)),
                () => LanguageExt.HashMap<Ulid, Lst<BaseTaskEvent>>.Empty));
            return Unit.Default;
        }, () => Unit.Default);
    }

    private readonly Channel<bool> _changeLocker =
        Channel.CreateBounded<bool>(1);

    private readonly bool _trackChanges;

    public TaskState GetTaskState()
    {
        
            return new(
                _assignedTasks.Value.Select(a => (a.Key, a.Value.ToHashMap()))
                    .ToHashMap(),
                _requestedTasks.Value.Select(a => (a.Key, a.Value.ToHashMap()))
                    .ToHashMap(),
                _unassignedTasks.Value.Select(a => (a.Key, a.Value.ToHashMap()))
                    .ToHashMap());
        
    }

    [MessagePackObject(keyAsPropertyName:true)]
    public record TaskState(
        HashMap<string, HashMap<Ulid, Lst<BaseTaskEvent>>> AssignedTasks,
        HashMap<string, HashMap<Ulid, Lst<BaseTaskEvent>>> RequestedTasks,
        HashMap<string, HashMap<Ulid, Lst<BaseTaskEvent>>> UnassignedTasks);

    private static (
        HashMap<TKeyOuter, Change<TrackingHashMap<TKeyInner, Lst<TLst>>>> chg,
        TrackingHashMap<TKeyOuter, TrackingHashMap<TKeyInner, Lst<TLst>>> thm,
        HashMap<TKeyOuter, HashMap<TKeyInner, Change<Lst<TLst>>>> innerChg)
        MakeSnapshot<TKeyOuter,TKeyInner,TLst>(
            TrackingHashMap<TKeyOuter, TrackingHashMap<TKeyInner, Lst<TLst>>> thm)
    {
        //TODO: This is trasshhhhhhh.
        var chg = thm.Changes;
        var preMap = thm.Map(a =>
        {
            var (k, v) = a;
            var c = v.Changes;
            return (k, (c, v.Snapshot()));
        }).ToHashMap();
        thm = thm.SetItems(preMap.Select(a => (a.Item1, a.Item2.Item2)));
        var innerChg = preMap.Select(a => (a.Item1, a.Item2.Item1))
            .ToHashMap();
        return (chg, thm, innerChg);
    }

    private void HandleTaskUpdate(TaskUpdate tu)
    {
        TUSwap(_assignedTasks,tu);
        TUSwap(_requestedTasks,tu);
        TUSwap(_unassignedTasks,tu);
    }

    private void HandleTaskCanceled(TaskCanceled taskCanceled)
    {

        TCanSwap(_assignedTasks,taskCanceled);
        TCanSwap(_requestedTasks,taskCanceled);
        TCanSwap(_unassignedTasks,taskCanceled);
    }

    private void HandleTaskCompleted(TaskCompleted taskCompleted)
    {

        TComSwap(_assignedTasks, taskCompleted);
        TComSwap(_unassignedTasks, taskCompleted);
        TComSwap(_requestedTasks, taskCompleted);
    }

    private void HandleTaskAssigned(TaskAssigned ta)
    {
        if (ta.UserId == UserId)
        {
            _assignedTasks.Swap(ta, (tai, thm) =>
            {
                return thm.AddOrUpdate(tai.TaskType, tai.TaskId,
                    e => e.Add(tai),
                    () => Lst<BaseTaskEvent>.Empty.Add(tai));
            });
        }
        else
        {
            _assignedTasks.Swap(ta, (tai, thm) =>
            {
                return thm.Remove(tai.TaskType, tai.TaskId);
            });
        }
    }

    private void HandleTaskBegan(TaskBegan tb)
    {
        _requestedTasks.Swap(tb, (tbi, thm) =>
        {
            return thm.AddOrUpdate(tbi.TaskType, tbi.TaskId, t =>
                t.Add(tbi), () =>
                        Lst<BaseTaskEvent>.Empty.Add(tbi));
        });
    }

    public async IAsyncEnumerable<BaseTaskEvent> BeginNewEventConsumer(CancellationToken token = default)
    {
        {
            await foreach (var msg in (_client.GetConsumer<BaseTaskEvent>(
                         SubjectHelpers.TaskUserSub(UserId),token: token))
                               .WithCancellation(token))
            {
                yield return msg.Data!;
                await msg.AckAsync(token);
            }
        }
        
    }

    public async ValueTask ConsumeSnap(TaskSerializationState row)
    {
        var newAMap = HashMap<string, HashMap<Ulid, Lst<BaseTaskEvent>>>();
        var newRMap = HashMap<string, HashMap<Ulid, Lst<BaseTaskEvent>>>();
        var newUaMap = HashMap<string, HashMap<Ulid, Lst<BaseTaskEvent>>>();
        foreach (var valueTuple in row.Assigned)
        {
            var catAMap = HashMap<Ulid, Lst<BaseTaskEvent>>();
            var catRMap = HashMap<Ulid, Lst<BaseTaskEvent>>();
            foreach (var inner in valueTuple.Value)
            {
                var state = GetTaskState(inner.Value);
                if (state.IsAssignedToUser)
                {
                    catAMap = catAMap.Add(inner.Key, inner.Value);
                }
                if (state.IsRequestedByUser)
                {
                    catRMap = catRMap.Add(inner.Key, inner.Value);
                }
            }

            newAMap = newAMap.Add(valueTuple.Key, catAMap);
            newRMap = newRMap.Add(valueTuple.Key, catRMap);
        }

        foreach (var valueTuple in row.Unassigned)
        {
            var catRMap = newRMap.Find(valueTuple.Key, a => a, () => default);
            var catUaMap = HashMap<Ulid, Lst<BaseTaskEvent>>();
            foreach (var inner in valueTuple.Value)
            {
                var state = GetTaskState(inner.Value);
                if (state.IsRequestedByUser)
                {
                    catRMap = catRMap.Add(inner.Key, inner.Value);
                }

                catUaMap = catUaMap.Add(inner.Key, inner.Value);
            }

            newRMap = newAMap.SetItem(valueTuple.Key, catRMap);
            newUaMap = newUaMap.SetItem(valueTuple.Key, catUaMap);
        }

        _assignedTasks.Swap(newAMap, (arg, ex) => arg);
        _unassignedTasks.Swap(newUaMap, (arg, ex) => arg);
        _requestedTasks.Swap(newRMap, (arg, ex) => arg);
        //row.Assigned.Where(a=>a.)
    }

    private RebuiltTaskState GetTaskState(Lst<BaseTaskEvent> row)
    {
        string assignedUserId = "";
        string requestedUserId = "";
        foreach (var baseTaskEvent in row)
        {
            if (baseTaskEvent is TaskRequest taskRequest)
            {
                requestedUserId = taskRequest.RequestingUserId;
            }
            else if (baseTaskEvent is TaskAssigned ta)
            {
                assignedUserId = ta.UserId;
            }
        }

        return new(assignedUserId == UserId, requestedUserId == UserId);
    }

    public async ValueTask ConsumeEvent(BaseTaskEvent row)
    {
        this.ProcessTaskEventSwitch(row);
    }
}

/// <summary>
/// 
/// </summary>
/// <remarks>
/// Ok. So the way this works, is we use a series of <see cref="Atom{A}"/>
/// Hashmaps of Maps. Atom lets us Swap via interlocked,
/// such that if the values being swapped are 'comparable' and the actions are
/// 'safely repeatable', it can repeat the action if a concurrent update happens
/// until the interlocked operation succeeds.
/// </remarks>
public class TaskSetCoordinatorHashMap : ISnapshotAndEventConsumer
{
    public string UserId { get; }

    public TaskSetCoordinatorHashMap(string userId,
        ClientBootstrap clientBootstrap, bool trackChanges)
    {
        UserId = userId;
        _client = clientBootstrap;
        _trackChanges = trackChanges;
        _assignedTasks = Atom(HashMap<string, HashMap<Ulid, Lst<BaseTaskEvent>>>());
        _requestedTasks =  Atom(HashMap<string, HashMap<Ulid, Lst<BaseTaskEvent>>>());
        _unassignedTasks = Atom(HashMap<string, HashMap<Ulid, Lst<BaseTaskEvent>>>());
    }
    
    private readonly Atom<HashMap<string, HashMap<Ulid, Lst<BaseTaskEvent>>>> _assignedTasks;

    private readonly Atom<HashMap<string, HashMap<Ulid, Lst<BaseTaskEvent>>>> _requestedTasks;

    private readonly Atom<HashMap<string, HashMap<Ulid, Lst<BaseTaskEvent>>>> _unassignedTasks;

    private readonly ClientBootstrap _client;
    private Task _mainTask;

    public async Task<Ulid> CreateTask(string taskType, string text,
        string? assignedToUser, DateTimeOffset expectedBy)
    {
        var taskId = Ulid.NewUlid();
        await _client.SendMessage(new TaskRequest(Ulid.NewUlid(), UserId, assignedToUser,
            taskId, taskType, text, DateTimeOffset.UtcNow,
            expectedBy));
        return taskId;
    }

    public async ValueTask AddTaskNote(TaskRequest origReq,
        string notes)
    {
        await _client.SendMessage(new TaskUpdate(Ulid.NewUlid(),origReq.TaskId,
            origReq.TaskType, UserId, notes));
    }

    public async ValueTask<bool> AddTaskCanceled(TaskRequest origReq, string notes)
    {
        if (origReq.RequestingUserId == UserId)
        {
            await _client.SendMessage(new TaskCanceled(Ulid.NewUlid(),origReq.TaskId,
                origReq.TaskType, UserId, notes, DateTimeOffset.Now));
            return true;
        }

        return false;
    }

    public async ValueTask<ReassignResult> ReassignTask(Ulid taskId,
        string newUser, string notes)
    {
        bool foundTask = false;
        foreach (var requestedTask in _assignedTasks.Value)
        {
            var item =
                requestedTask.Value.Find(taskId, a => a,
                    static () => Lst<BaseTaskEvent>.Empty);
            if (item != default)
            {
                foundTask = true;
                await _client.SendMessage(new TaskAssigned(Ulid.NewUlid(),taskId, newUser,
                    requestedTask.Key, UserId, notes, item,
                    DateTimeOffset.Now));
                //_assignedTasks.AddOrUpdate(requestedTask.Key,
                //    e => e.Remove(taskId),
                //    () => HashMap<Ulid, Lst<BaseTaskEvent>>.Empty);
                return new ReassignResult(taskId, true, "Reassigned");
            }
        }

        foreach (var requestedTask in _unassignedTasks.Value)
        {
            var item =
                requestedTask.Value.Find(taskId, a => a,
                    static () => Lst<BaseTaskEvent>.Empty);
            if (item != default)
            {
                foundTask = true;
                await _client.SendMessage(new TaskAssigned(Ulid.NewUlid(),taskId, newUser,
                    requestedTask.Key, UserId, notes, item,
                    DateTimeOffset.Now));
                //_assignedTasks.AddOrUpdate(requestedTask.Key,
                //    e => e.Remove(taskId),
                //    () => HashMap<Ulid, Lst<BaseTaskEvent>>.Empty);
                return new ReassignResult(taskId, true, "Reassigned");
            }
        }

        return new ReassignResult(taskId, false, "Could not find task!");
    }

    public async ValueTask Init(TaskCompletionSource initLock,
        CancellationToken token)
    {
        var userEvents =
            await _client.Sub<BaseTaskEvent>(
                SubjectHelpers.TaskUserSub(UserId), token);
        CancellationTokenSource cts = null;
        _mainTask = Task.Run(async () =>
        {
            await initLock.Task;
            try
            {
                await foreach (var entry in userEvents.Msgs.ReadAllAsync(
                                   token))
                {
                    try
                    {
                        RunEventHandlers(entry);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                        throw;
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
            finally
            {
                try
                {
                    cts?.Cancel(false);
                }
                finally
                {
                    try
                    {
                        cts?.Dispose();
                    }
                    catch
                    {
                        // ignored
                    }
                }
            }
        });
    }

    private void RunEventHandlers(NatsMsg<BaseTaskEvent> entry)
    {
        ProcessTaskEventSwitch(entry.Data);
    }

    private void ProcessTaskEventSwitch(BaseTaskEvent? msg)
    {
        switch (msg)
        {
            case TaskAssigned ta:
            {
                HandleTaskAssigned(ta);

                break;
            }
            case TaskBegan tb:
            {
                HandleTaskBegan(tb);
                break;
            }
            case TaskCompleted tc:
            {
                HandleTaskCompleted(tc);
                break;
            }
            case TaskRequest tr:
            {
                HandleTaskRequest(tr);
                break;
            }
            case TaskUpdate tu:
            {
                HandleTaskUpdate(tu);
                break;
            }
            case TaskCanceled tcan:
            {
                HandleTaskCanceled(tcan);
                break;
            }

        }
    }

    public async ValueTask<ClearResult> ClearItem(Ulid taskId)
    {
        bool? foundAndCleared = null;
        _requestedTasks.Swap(thm =>
        {
            var innerThm = thm;
            foreach (var valueTuple in thm)
            {
                valueTuple.Value.Find(taskId, s =>
                {
                    var f = s.Head();
                    var c = s.Reverse().FirstOrDefault();
                    if (c is TaskCanceled or TaskCompleted ||
                        f is not TaskRequest)
                    {
                        innerThm =  innerThm.AddOrUpdate(valueTuple.Key,
                            thmi => thmi.Remove(taskId),
                            () => LanguageExt.HashMap<Ulid, Lst<BaseTaskEvent>>.Empty);
                    }

                    return true;
                }, () => false);
            }

            return innerThm;
        });
        _requestedTasks.Swap(thm =>
        {
            var innerThm = thm;
            foreach (var valueTuple in thm)
            {
                valueTuple.Value.Find(taskId, s =>
                {
                    var f = s.Head();
                    var c = s.Reverse().FirstOrDefault();
                    if (c is TaskCanceled or TaskCompleted ||
                        f is not TaskRequest)
                    {
                        innerThm =  innerThm.AddOrUpdate(valueTuple.Key,
                            thmi => thmi.Remove(taskId),
                            () => LanguageExt.HashMap<Ulid, Lst<BaseTaskEvent>>.Empty);
                    }

                    return true;
                }, () => false);
            }

            return innerThm;
        });
        _unassignedTasks.Swap(thm =>
        {
            var innerThm = thm;
            foreach (var valueTuple in thm)
            {
                valueTuple.Value.Find(taskId, s =>
                {
                    var f = s.Head();
                    var c = s.Reverse().FirstOrDefault();
                    if (c is TaskCanceled or TaskCompleted ||
                        f is not TaskRequest)
                    {
                        innerThm =  innerThm.AddOrUpdate(valueTuple.Key,
                            thmi => thmi.Remove(taskId),
                            () => LanguageExt.HashMap<Ulid, Lst<BaseTaskEvent>>.Empty);
                    }

                    return true;
                }, () => false);
            }

            return innerThm;
        });
        if (foundAndCleared.HasValue == false)
        {
            return new ClearResult(true, "no instances found");
        }
        else
        {
            if (foundAndCleared.Value)
            {
                return new ClearResult(true, "cleared");
            }
            else
            {
                return new ClearResult(false, "Tasks not in closed state");
            }
        }
    }

    private void HandleTaskRequest(TaskRequest trb)
    {
        void doSwap(
            Atom<HashMap<string,
                HashMap<Ulid, Lst<BaseTaskEvent>>>> taskSet,
            TaskRequest taskRequest)
        {
            taskSet.Swap(taskRequest, (tr,b)=>b.AddOrUpdate(tr.TaskType,
                (hm) => hm.TryAdd(tr.TaskId, Lst<BaseTaskEvent>.Empty.Add(tr)),
                () => LanguageExt.HashMap<Ulid, Lst<BaseTaskEvent>>.Empty.Add(tr.TaskId,
                    Lst<BaseTaskEvent>.Empty.Add(tr))));
        }

        if (trb.RequestingUserId == UserId)
        {
            doSwap(_requestedTasks, trb);
        }

        if (trb.AssignedUserId == UserId)
        {
            doSwap(_assignedTasks,trb);
        }
        else if (trb.AssignedUserId == "unassigned")
        {
            doSwap(_unassignedTasks,trb);
        }
    }

    private static void TUSwap(
        Atom<HashMap<string, HashMap<Ulid, Lst<BaseTaskEvent>>>>
            atom, TaskUpdate tu)
    {
        atom.Value.Find(tu.TaskType, hm =>
        {
            atom.Swap(b=>b.AddOrUpdate(tu.TaskType,
                (e) => e.AddOrUpdate(tu.TaskId, (s) => s.Add(tu),
                    () => Lst<BaseTaskEvent>.Empty.Add(tu)),
                () => LanguageExt.HashMap<Ulid, Lst<BaseTaskEvent>>.Empty));
            return Unit.Default;
        }, () => Unit.Default);
    }
    private static void TCanSwap(
        Atom<HashMap<string, HashMap<Ulid, Lst<BaseTaskEvent>>>>
            atom, TaskCanceled tu)
    {
        atom.Value.Find(tu.TaskType, hm =>
        {
            atom.Swap(b=>b.AddOrUpdate(tu.TaskType,
                (e) => e.AddOrUpdate(tu.TaskId, (s) => s.Add(tu),
                    () => Lst<BaseTaskEvent>.Empty.Add(tu)),
                () => LanguageExt.HashMap<Ulid, Lst<BaseTaskEvent>>.Empty));
            return Unit.Default;
        }, () => Unit.Default);
    }
    private static void TComSwap(
        Atom<HashMap<string, HashMap<Ulid, Lst<BaseTaskEvent>>>>
            atom, TaskCompleted tu)
    {
        atom.Value.Find(tu.TaskType, hm =>
        {
            atom.Swap(b=>b.AddOrUpdate(tu.TaskType,
                (e) => e.AddOrUpdate(tu.TaskId, (s) => s.Add(tu),
                    () => Lst<BaseTaskEvent>.Empty.Add(tu)),
                () => LanguageExt.HashMap<Ulid, Lst<BaseTaskEvent>>.Empty));
            return Unit.Default;
        }, () => Unit.Default);
    }

    private readonly Channel<bool> _changeLocker =
        Channel.CreateBounded<bool>(1);

    private readonly bool _trackChanges;

    public TaskState GetTaskState()
    {
        
            return new(
                _assignedTasks.Value.Select(a => (a.Key, a.Value.ToHashMap()))
                    .ToHashMap(),
                _requestedTasks.Value.Select(a => (a.Key, a.Value.ToHashMap()))
                    .ToHashMap(),
                _unassignedTasks.Value.Select(a => (a.Key, a.Value.ToHashMap()))
                    .ToHashMap());
        
    }

    [MessagePackObject(keyAsPropertyName:true)]
    public record TaskState(
        HashMap<string, HashMap<Ulid, Lst<BaseTaskEvent>>> AssignedTasks,
        HashMap<string, HashMap<Ulid, Lst<BaseTaskEvent>>> RequestedTasks,
        HashMap<string, HashMap<Ulid, Lst<BaseTaskEvent>>> UnassignedTasks);

    private static (
        HashMap<TKeyOuter, Change<TrackingHashMap<TKeyInner, Lst<TLst>>>> chg,
        TrackingHashMap<TKeyOuter, TrackingHashMap<TKeyInner, Lst<TLst>>> thm,
        HashMap<TKeyOuter, HashMap<TKeyInner, Change<Lst<TLst>>>> innerChg)
        MakeSnapshot<TKeyOuter,TKeyInner,TLst>(
            TrackingHashMap<TKeyOuter, TrackingHashMap<TKeyInner, Lst<TLst>>> thm)
    {
        //TODO: This is trasshhhhhhh.
        var chg = thm.Changes;
        var preMap = thm.Map(a =>
        {
            var (k, v) = a;
            var c = v.Changes;
            return (k, (c, v.Snapshot()));
        }).ToHashMap();
        thm = thm.SetItems(preMap.Select(a => (a.Item1, a.Item2.Item2)));
        var innerChg = preMap.Select(a => (a.Item1, a.Item2.Item1))
            .ToHashMap();
        return (chg, thm, innerChg);
    }

    private void HandleTaskUpdate(TaskUpdate tu)
    {
        TUSwap(_assignedTasks,tu);
        TUSwap(_requestedTasks,tu);
        TUSwap(_unassignedTasks,tu);
    }

    private void HandleTaskCanceled(TaskCanceled taskCanceled)
    {

        TCanSwap(_assignedTasks,taskCanceled);
        TCanSwap(_requestedTasks,taskCanceled);
        TCanSwap(_unassignedTasks,taskCanceled);
    }

    private void HandleTaskCompleted(TaskCompleted taskCompleted)
    {

        TComSwap(_assignedTasks, taskCompleted);
        TComSwap(_unassignedTasks, taskCompleted);
        TComSwap(_requestedTasks, taskCompleted);
    }

    private void HandleTaskAssigned(TaskAssigned ta)
    {
        if (ta.UserId == UserId)
        {
            _assignedTasks.Swap(ta, (tai, thm) =>
            {
                return thm.AddOrUpdate(tai.TaskType, tai.TaskId,
                    e => e.Add(tai),
                    () => Lst<BaseTaskEvent>.Empty.Add(tai));
            });
        }
        else
        {
            _assignedTasks.Swap(ta, (tai, thm) =>
            {
                return thm.Remove(tai.TaskType, tai.TaskId);
            });
        }
    }

    private void HandleTaskBegan(TaskBegan tb)
    {
        _requestedTasks.Swap(tb, (tbi, thm) =>
        {
            return thm.AddOrUpdate(tbi.TaskType, tbi.TaskId, t =>
                t.Add(tbi), () =>
                        Lst<BaseTaskEvent>.Empty.Add(tbi));
        });
    }

    public async IAsyncEnumerable<BaseTaskEvent> BeginNewEventConsumer(CancellationToken token = default)
    {
        await using (var userEvents =
                     await _client.Sub<BaseTaskEvent>(
                         SubjectHelpers.TaskUserSub(UserId), token))
        {
            await foreach (var msg in userEvents.Msgs.ReadAllAsync(token)
                               .WithCancellation(token))
            {
                yield return msg.Data!;
            }
        }
        
    }

    public async ValueTask ConsumeSnap(TaskSerializationState row)
    {
        var newAMap = HashMap<string, HashMap<Ulid, Lst<BaseTaskEvent>>>();
        var newRMap = HashMap<string, HashMap<Ulid, Lst<BaseTaskEvent>>>();
        var newUaMap = HashMap<string, HashMap<Ulid, Lst<BaseTaskEvent>>>();
        foreach (var valueTuple in row.Assigned)
        {
            var catAMap = HashMap<Ulid, Lst<BaseTaskEvent>>();
            var catRMap = HashMap<Ulid, Lst<BaseTaskEvent>>();
            foreach (var inner in valueTuple.Value)
            {
                var state = GetTaskState(inner.Value);
                if (state.IsAssignedToUser)
                {
                    catAMap = catAMap.Add(inner.Key, inner.Value);
                }
                if (state.IsRequestedByUser)
                {
                    catRMap = catRMap.Add(inner.Key, inner.Value);
                }
            }

            newAMap = newAMap.Add(valueTuple.Key, catAMap);
            newRMap = newRMap.Add(valueTuple.Key, catRMap);
        }

        foreach (var valueTuple in row.Unassigned)
        {
            var catRMap = newRMap.Find(valueTuple.Key, a => a, () => default);
            var catUaMap = HashMap<Ulid, Lst<BaseTaskEvent>>();
            foreach (var inner in valueTuple.Value)
            {
                var state = GetTaskState(inner.Value);
                if (state.IsRequestedByUser)
                {
                    catRMap = catRMap.Add(inner.Key, inner.Value);
                }

                catUaMap = catUaMap.Add(inner.Key, inner.Value);
            }

            newRMap = newAMap.SetItem(valueTuple.Key, catRMap);
            newUaMap = newUaMap.SetItem(valueTuple.Key, catUaMap);
        }

        _assignedTasks.Swap(newAMap, (arg, ex) => arg);
        _unassignedTasks.Swap(newUaMap, (arg, ex) => arg);
        _requestedTasks.Swap(newRMap, (arg, ex) => arg);
        //row.Assigned.Where(a=>a.)
    }

    private RebuiltTaskState GetTaskState(Lst<BaseTaskEvent> row)
    {
        string assignedUserId = "";
        string requestedUserId = "";
        foreach (var baseTaskEvent in row)
        {
            if (baseTaskEvent is TaskRequest taskRequest)
            {
                requestedUserId = taskRequest.RequestingUserId;
            }
            else if (baseTaskEvent is TaskAssigned ta)
            {
                assignedUserId = ta.UserId;
            }
        }

        return new(assignedUserId == UserId, requestedUserId == UserId);
    }

    public async ValueTask ConsumeEvent(BaseTaskEvent row)
    {
        this.ProcessTaskEventSwitch(row);
    }
}

public record RebuiltTaskState(bool IsAssignedToUser, bool IsRequestedByUser);

[MessagePackObject(keyAsPropertyName:true)]
public record ReassignResult(Ulid TaskId, bool Success, string Message);

[MessagePackObject(keyAsPropertyName:true)]
public record ClearResult(bool Success, string? Reason);

public class ChatTaskClientSerializer<T> : INatsSerializer<T>
{
    public static readonly ChatTaskClientSerializer<T> Default =
        new ChatTaskClientSerializer<T>();

    private static readonly IFormatterResolver DefaultFormatterResolver =
        CompositeResolver.Create(new[]
        {
            UlidMessagePackResolver.Instance,
            MessagePackSerializerOptions.Standard.Resolver
        });

    public static readonly MessagePackSerializerOptions DefaultSerializerOptions =
        MessagePackSerializerOptions.Standard.WithResolver(
            DefaultFormatterResolver);
    public void Serialize(IBufferWriter<byte> bufferWriter, T value)
    {
        MessagePackSerializer.Serialize(bufferWriter, value,
            DefaultSerializerOptions);
    }

    public T? Deserialize(in ReadOnlySequence<byte> buffer)
    {
        return MessagePackSerializer.Deserialize<T>(buffer,
            DefaultSerializerOptions);
    }
}

public interface ISubjectFactory
{
    string GetSubject();
}

[Union(2, typeof(TaskRequest))]
[Union(3, typeof(TaskCompleted))]
[Union(4, typeof(TaskBegan))]
[Union(5, typeof(TaskUpdate))]
[Union(6, typeof(TaskAssigned))]
[Union(7, typeof(TaskCanceled))]
[MessagePackObject(keyAsPropertyName:true)]
public abstract record BaseTaskEvent(Ulid EventId, Ulid TaskId, string TaskType)
    : ISubjectFactory, IEquatable<BaseTaskEvent>
{
    bool IEquatable<BaseTaskEvent>.Equals(BaseTaskEvent? other)
    {
        return other != null && other.EventId == EventId;
    }
    public abstract string GetSubject();
}




public static class SubjectHelpers
{
    public static string BroadcastSubject =>
        $"sample.chat.message.broadcast";

    public static string DirectMessage(string from, string to) =>
        $"sample.chat.message.direct.{from}.{to}";

    public static string
        Tasks(string scope, string type, Ulid taskId, string requested,
            string assigned) =>
        $"sample.task.{scope}.{type},{taskId}.{requested}.{assigned}";

    public static string Tasks(string scope, string type, Ulid taskId,
        string doneByUser)
    {
        return $"sample.task.{scope}.{type},{taskId}.{doneByUser}";
    }

    public static string TaskUserSub(string userId)
    {
        return $"sample.task.>";
    }

    public static string TaskReplication => $"sample.task.>";

}



[MessagePackObject(true)]
public record TaskRequest(
    Ulid EventId,
    string RequestingUserId,
    string? AssignedUserId,
    Ulid TaskId,
    string TaskType,
    string Text,
    DateTimeOffset RequestedAt,
    DateTimeOffset ExpectedBy) : BaseTaskEvent(EventId,TaskId,TaskType)
{
    public override string GetSubject()
    {
        return SubjectHelpers.Tasks(TaskScopeSubjects.Request, TaskType, TaskId,
            RequestingUserId,
            AssignedUserId != null ? AssignedUserId : "unassigned");
    }
}

[MessagePackObject(true)]
public record TaskCanceled(
    Ulid EventId,
    Ulid TaskId,
    string TaskType,
    string CanceledByUser,
    string CancelReason,
    DateTimeOffset CancelTime) : BaseTaskEvent(EventId,TaskId,TaskType) 
{
    public override string GetSubject()
    {
        return SubjectHelpers.Tasks(TaskScopeSubjects.Canceled, TaskType, TaskId,
            CanceledByUser);
    }
}

[MessagePackObject(true)]
public record TaskBegan(
    Ulid EventId,
    Ulid TaskId,
    string TaskType,
    string UserId,
    string Text,
    DateTimeOffset BeginTime) : BaseTaskEvent(EventId, TaskId, TaskType)
{
    public override string GetSubject()
    {
        return SubjectHelpers.Tasks(TaskScopeSubjects.Begun, TaskType, TaskId, UserId);
    }
}

[MessagePackObject(true)]
public record TaskUpdate(
    Ulid EventId,
    Ulid TaskId,
    string TaskType,
    string UserId,
    string Notes) : BaseTaskEvent(EventId,TaskId,TaskType)
{
    public override string GetSubject()
    {
        return SubjectHelpers.Tasks(TaskScopeSubjects.Update, TaskType, TaskId, UserId);
    }
}

[MessagePackObject(true)]
public record TaskCompleted(
    Ulid EventId,
    Ulid TaskId,
    string TaskType,
    string UserId,
    string Notes,
    DateTimeOffset CompletedTime) : BaseTaskEvent(EventId,TaskId,TaskType)
{
    public override string GetSubject()
    {
        return SubjectHelpers.Tasks(TaskScopeSubjects.Completed, TaskType, TaskId, UserId);
    }
}

public static class TaskScopeSubjects
{
    public static readonly string Completed = "completed";
    public static readonly string Assigned = "assigned";
    public static readonly string Begun = "begun";
    public static readonly string Update = "update";
    public static readonly string Canceled = "canceled";
    public static readonly string Request = "request";
}

[MessagePackObject(true)]
public record TaskAssigned(
    Ulid EventId,
    Ulid TaskId,
    string TaskType,
    string UserId,
    string AssignedByUserId,
    string Notes,
    Lst<BaseTaskEvent> History,
    DateTimeOffset AssignedTime
) : BaseTaskEvent(EventId, TaskId, TaskType)
{
    public override string GetSubject()
    {
        return SubjectHelpers.Tasks(TaskScopeSubjects.Assigned, TaskType, TaskId, UserId,
            AssignedByUserId);
    }
}


public class ClientCoordinator
{
    public string UserId { get; }
    private readonly Atom<ChatHistory> _broadcastHistory;
    private readonly Atom<DmConversationSet> _directMessageHistory;
    private readonly ClientBootstrap _client;

    public ClientCoordinator(string userId, ClientBootstrap bootstrap)
    {
        UserId = userId;
        _client = bootstrap;
        // OK so the way Atoms work, is they use a 'swap' function,
        // and then check for equality/etc.
        // If your type does equality (i.e. records)
        // and your underlying types are immutable,
        // and your actions are able to be applied...
        // magic! in case of a conflict, the operation is re-attempted,
        // all in a thread safe fashion (as long as your swap is thread safe)
        _broadcastHistory =
            Atom(new ChatHistory(default));
        _directMessageHistory =
            Atom(new DmConversationSet(LanguageExt
                .HashMap<string, Lst<DirectMessage>>.Empty));
    }

    public async ValueTask Begin(CancellationToken token)
    {
        
        var (broadcastSub, dmSub, outboundDmSub) = await ValueTaskEx.WhenAll(
            _client.SubChat<BroadcastMessage>(
                SubjectHelpers.BroadcastSubject,
                token),
            _client.SubChat<DirectMessage>(
                SubjectHelpers.DirectMessage("*",UserId),
                token),
            _client.SubChat<DirectMessage>(
                SubjectHelpers.DirectMessage(UserId,"*"), token)
        );
        Task.Run(async () =>
        {
            try
            {
                await foreach (var reader in broadcastSub.Msgs.ReadAllAsync(
                                   token))
                {
                    _broadcastHistory.Swap(reader.Data!,
                        (m, h) => h.AddMsg(m));
                }
            }
            catch
            {
            }
        });
        Task.Run(async () =>
        {
            try
            {
                await foreach
                    (var reader in outboundDmSub.Msgs.ReadAllAsync(
                        token))
                {
                    _directMessageHistory.Swap(reader.Data!,
                        (m, h) =>
                        {
                            return h.AddOrMarkMessageReceived(m.ToUserId,
                                m);
                        });
                }
            }
            catch
            {
            }
        });
        Task.Run(async () =>
        {
            try
            {
                await foreach (var reader in dmSub.Msgs.ReadAllAsync(
                                   token))
                {
                    _directMessageHistory.Swap(reader.Data!, UserId,
                        (m, h, v) =>
                        {
                            if (m.FromUserId != h)
                            {
                                return v.AddMsg(m.FromUserId, m);
                            }
                            else
                            {
                                return v.AddMsg(m.ToUserId, m);
                            }
                        });
                }
            }
            catch
            {
            }
        });
    }
    
    
}
[Union(0, typeof(BroadcastMessage))]
[Union(1, typeof(DirectMessage))]
public interface IChatTaskClient : ISubjectFactory
{
}

[MessagePackObject(true)]
public record BroadcastMessage(
    string FromUserId,
    string Text) : IChatTaskClient
{
    public DateTimeOffset TimeSent { get; set; }

    public string GetSubject()
    {
        return SubjectHelpers.BroadcastSubject;
    }
}
public record DmConversationSet(HashMap<string, Lst<DirectMessage>> User)
{
    public DmConversationSet AddMsg(string arg1FromUserId,
        DirectMessage directMessage)
    {
        return this with
        {
            User = User.AddOrUpdate(arg1FromUserId,
                (a) => a.Contains(directMessage) ? a : a.Add(directMessage),
                () => new Lst<DirectMessage>().Add(directMessage))
        };
    }

    public DmConversationSet AddOrMarkMessageReceived(string arg1ToUserId,
        DirectMessage directMessage)
    {
        return AddMsg(arg1ToUserId, directMessage);
    }
}
[MessagePackObject(true)]
public record DirectMessage(
    string FromUserId,
    string ToUserId,
    string Text) : IChatTaskClient
{
    public string GetSubject()
    {
        return SubjectHelpers.DirectMessage(FromUserId, ToUserId);
    }
}