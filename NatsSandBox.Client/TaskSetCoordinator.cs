using System.Threading.Channels;
using LanguageExt;
using NATS.Client.Core;

namespace NatsSandBox.Client;

public class SnapshotBuild
{
    internal HashMap<string, Change<TrackingHashMap<Ulid, Lst<BaseTaskEvent>>>> AssignedTasks;
    internal HashMap<string, Change<TrackingHashMap<Ulid, Lst<BaseTaskEvent>>>> RequestedTasks;
    internal HashMap<string, Change<TrackingHashMap<Ulid, Lst<BaseTaskEvent>>>> UnassignedTasks;
    internal HashMap<string, HashMap<Ulid, Change<Lst<BaseTaskEvent>>>> AssignedInner;
    internal HashMap<string, HashMap<Ulid, Change<Lst<BaseTaskEvent>>>> RequestedInner;
    internal HashMap<string, HashMap<Ulid, Change<Lst<BaseTaskEvent>>>> UnassignedInner;

    public TaskChangeSnapshot ToRecord() =>
        new TaskChangeSnapshot(AssignedTasks, RequestedTasks, UnassignedTasks, AssignedInner, RequestedInner, UnassignedInner);
}

public class TaskSetCoordinator
{
    public string UserId { get; }

    public TaskSetCoordinator(string userId,
        ClientBootstrap clientBootstrap, bool trackChanges)
    {
        UserId = userId;
        _client = clientBootstrap;
        _trackChanges = trackChanges;
        _assignedTasks = Prelude.Atom(Prelude
            .TrackingHashMap<string, TrackingHashMap<Ulid, Lst<BaseTaskEvent>>>());
        _requestedTasks = Prelude.Atom(Prelude
            .TrackingHashMap<string, TrackingHashMap<Ulid, Lst<BaseTaskEvent>>>());
        _unassignedTasks = Prelude.Atom(Prelude
            .TrackingHashMap<string, TrackingHashMap<Ulid, Lst<BaseTaskEvent>>>());
    }

    private readonly
        Atom<TrackingHashMap<string, TrackingHashMap<Ulid, Lst<BaseTaskEvent>>>>
        _assignedTasks;

    private readonly
        Atom<TrackingHashMap<string, TrackingHashMap<Ulid, Lst<BaseTaskEvent>>>>
        _requestedTasks;

    private Atom<
            TrackingHashMap<string, TrackingHashMap<Ulid, Lst<BaseTaskEvent>>>>
        _unassignedTasks;

    private readonly ClientBootstrap _client;
    private Task _mainTask;

    public async ValueTask CreateTask(string taskType, string text,
        string? assignedToUser, DateTimeOffset expectedBy)
    {
        await _client.SendMessage(new TaskRequest(Ulid.NewUlid(), UserId,
            assignedToUser,
            Ulid.NewUlid(), taskType, text, DateTimeOffset.UtcNow,
            expectedBy));
    }

    public async ValueTask AddTaskNote(TaskRequest origReq,
        string notes)
    {
        await _client.SendMessage(new TaskUpdate(Ulid.NewUlid(), origReq.TaskId,
            origReq.TaskType, UserId, notes));
    }

    public async ValueTask<bool> AddTaskCanceled(TaskRequest origReq,
        string notes)
    {
        if (origReq.RequestingUserId == UserId)
        {
            await _client.SendMessage(new TaskCanceled(Ulid.NewUlid(),
                origReq.TaskId,
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
                await _client.SendMessage(new TaskAssigned(Ulid.NewUlid(),
                    taskId, newUser,
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
                await _client.SendMessage(new TaskAssigned(Ulid.NewUlid(),
                    taskId, newUser,
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

    public async ValueTask Init(CancellationToken token)
    {
        var userEvents =
            await _client.Sub<BaseTaskEvent>(
                SubjectHelpers.TaskUserSub(UserId), token);
        CancellationTokenSource cts = null;
        if (_trackChanges == false)
        {
            cts = new CancellationTokenSource();
            _ = Task.Factory.StartNew(async () =>
            {
                await foreach (var cl in ChangeLoop().WithCancellation(token))
                {
                    if (cts.IsCancellationRequested)
                    {
                        break;
                    }

                    await Task.Delay(5000);
                }
            });
        }

        _mainTask = Task.Run(async () =>
        {
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
        var msg = entry.Data;
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
                        innerThm = innerThm.AddOrUpdate(valueTuple.Key,
                            thmi => thmi.Remove(taskId),
                            () => LanguageExt
                                .TrackingHashMap<Ulid, Lst<BaseTaskEvent>>.Empty);
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
                        innerThm = innerThm.AddOrUpdate(valueTuple.Key,
                            thmi => thmi.Remove(taskId),
                            () => LanguageExt
                                .TrackingHashMap<Ulid, Lst<BaseTaskEvent>>.Empty);
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
                        innerThm = innerThm.AddOrUpdate(valueTuple.Key,
                            thmi => thmi.Remove(taskId),
                            () => LanguageExt
                                .TrackingHashMap<Ulid, Lst<BaseTaskEvent>>.Empty);
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
            Atom<TrackingHashMap<string,
                TrackingHashMap<Ulid, Lst<BaseTaskEvent>>>> taskSet,
            TaskRequest taskRequest)
        {
            taskSet.Swap(taskRequest, (tr, b) => b.AddOrUpdate(tr.TaskType,
                (hm) => hm.TryAdd(tr.TaskId, Lst<BaseTaskEvent>.Empty.Add(tr)),
                () => LanguageExt.TrackingHashMap<Ulid, Lst<BaseTaskEvent>>.Empty
                    .Add(tr.TaskId,
                        Lst<BaseTaskEvent>.Empty.Add(tr))));
        }

        if (trb.RequestingUserId == UserId)
        {
            doSwap(_requestedTasks, trb);
        }

        if (trb.AssignedUserId == UserId)
        {
            doSwap(_assignedTasks, trb);
        }
        else if (trb.AssignedUserId == "unassigned")
        {
            doSwap(_unassignedTasks, trb);
        }
    }

    private static void TUSwap(
        Atom<TrackingHashMap<string, TrackingHashMap<Ulid, Lst<BaseTaskEvent>>>>
            atom, TaskUpdate tu)
    {
        atom.Value.Find(tu.TaskType, hm =>
        {
            atom.Swap(b => b.AddOrUpdate(tu.TaskType,
                (e) => e.AddOrUpdate(tu.TaskId, (s) => s.Add(tu),
                    () => Lst<BaseTaskEvent>.Empty.Add(tu)),
                () => LanguageExt.TrackingHashMap<Ulid, Lst<BaseTaskEvent>>
                    .Empty));
            return Unit.Default;
        }, () => Unit.Default);
    }

    private static void TCanSwap(
        Atom<TrackingHashMap<string, TrackingHashMap<Ulid, Lst<BaseTaskEvent>>>>
            atom, TaskCanceled tu)
    {
        atom.Value.Find(tu.TaskType, hm =>
        {
            atom.Swap(b => b.AddOrUpdate(tu.TaskType,
                (e) => e.AddOrUpdate(tu.TaskId, (s) => s.Add(tu),
                    () => Lst<BaseTaskEvent>.Empty.Add(tu)),
                () => LanguageExt.TrackingHashMap<Ulid, Lst<BaseTaskEvent>>
                    .Empty));
            return Unit.Default;
        }, () => Unit.Default);
    }

    private static void TComSwap(
        Atom<TrackingHashMap<string, TrackingHashMap<Ulid, Lst<BaseTaskEvent>>>>
            atom, TaskCompleted tu)
    {
        atom.Value.Find(tu.TaskType, hm =>
        {
            atom.Swap(b => b.AddOrUpdate(tu.TaskType,
                (e) => e.AddOrUpdate(tu.TaskId, (s) => s.Add(tu),
                    () => Lst<BaseTaskEvent>.Empty.Add(tu)),
                () => LanguageExt.TrackingHashMap<Ulid, Lst<BaseTaskEvent>>
                    .Empty));
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

    public record TaskState(
        HashMap<string, HashMap<Ulid, Lst<BaseTaskEvent>>> AssignedTasks,
        HashMap<string, HashMap<Ulid, Lst<BaseTaskEvent>>> RequestedTasks,
        HashMap<string, HashMap<Ulid, Lst<BaseTaskEvent>>> UnassignedTasks);

    public async IAsyncEnumerable<TaskChangeSnapshot> ChangeLoop()
    {
        while (await _changeLocker.Reader.ReadAsync())
        {
            var snapshotLoop = new SnapshotBuild();
            _assignedTasks.Swap(snapshotLoop, (sl, thm) =>
            {
                (sl.AssignedTasks, thm, sl.AssignedInner) = MakeSnapshot(thm);
                return thm.Snapshot();
            });
            _unassignedTasks.Swap(snapshotLoop, (sl, thm) =>
            {
                (sl.RequestedTasks, thm, sl.RequestedInner) = MakeSnapshot(thm);
                return thm.Snapshot();
            });
            _requestedTasks.Swap(snapshotLoop, (sl, thm) =>
            {
                (sl.UnassignedTasks, thm, sl.UnassignedInner) =
                    MakeSnapshot(thm);
                return thm.Snapshot();
            });
            _changeLocker.Writer.TryWrite(true);
            yield return snapshotLoop.ToRecord();
        }
    }

    private static (
        HashMap<TKeyOuter, Change<TrackingHashMap<TKeyInner, Lst<TLst>>>> chg,
        TrackingHashMap<TKeyOuter, TrackingHashMap<TKeyInner, Lst<TLst>>> thm,
        HashMap<TKeyOuter, HashMap<TKeyInner, Change<Lst<TLst>>>> innerChg)
        MakeSnapshot<TKeyOuter, TKeyInner, TLst>(
            TrackingHashMap<TKeyOuter, TrackingHashMap<TKeyInner, Lst<TLst>>>
                thm)
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
        TUSwap(_assignedTasks, tu);
        TUSwap(_requestedTasks, tu);
        TUSwap(_unassignedTasks, tu);
    }

    private void HandleTaskCanceled(TaskCanceled taskCanceled)
    {
        TCanSwap(_assignedTasks, taskCanceled);
        TCanSwap(_requestedTasks, taskCanceled);
        TCanSwap(_unassignedTasks, taskCanceled);
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
            _assignedTasks.Swap(ta,
                (tai, thm) => { return thm.Remove(tai.TaskType, tai.TaskId); });
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
}
public class TaskChangeSnapshot(
    HashMap<string, Change<TrackingHashMap<Ulid, Lst<BaseTaskEvent>>>>
        AssignedTasks,
    HashMap<string, Change<TrackingHashMap<Ulid, Lst<BaseTaskEvent>>>>
        RequestedTasks,
    HashMap<string, Change<TrackingHashMap<Ulid, Lst<BaseTaskEvent>>>>
        UnassignedTasks,
    HashMap<string, HashMap<Ulid, Change<Lst<BaseTaskEvent>>>> AssignedInner,
    HashMap<string, HashMap<Ulid, Change<Lst<BaseTaskEvent>>>> RequestedInner,
    HashMap<string, HashMap<Ulid, Change<Lst<BaseTaskEvent>>>> UnassignedInner);

public class trash
{
    /*
    public async ValueTask StartAndApplyFromSnapshot(
        ISnapshotAndEventConsumer consumer)
    {
        var c = consumer.BeginNewEventConsumer();
        await RecoverImpl(consumer);

        await foreach (var a in c)
        {
            try
            {
                using (var ctx =
                       await _snapshotGrabbingLocker.WaitAndAcquireSafe(
                           (CancellationToken)default))
                {
                    await consumer.ConsumeEvent(a);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }
    }

    private async ValueTask RecoverImpl(ISnapshotAndEventConsumer consumer)
    {
        var s = await _replicationSqlReader.GetNewestSnapshot();
        if (s != null)
        {
            await consumer.ConsumeSnap(
                ReplicationSerializer.Instance.DeSerializeTaskSnapshot(
                    s!.TaskSnapshot));
        }

        var o = s?.LastOrdering ?? 0;
        await foreach (var a in _replicationSqlReader.BeginReadAtOrdering(o))
        {
            await consumer.ConsumeEvent(
                ReplicationSerializer.Instance.GetTaskEvent(a.TaskEventData));
        }
    }*/
    public readonly struct SemaphoreSuperSlimContext : IDisposable
{
    private readonly SemaphoreSuperSlim _semaphoreSuperSlim;

    public SemaphoreSuperSlimContext(SemaphoreSuperSlim parent)
    {
        _semaphoreSuperSlim = parent;
    }
    public void Dispose()
    {
        _semaphoreSuperSlim.TryRelease();
    }
}
    /// <summary>
    /// We use this because, unlike <see cref="SemaphoreSlim"/>,
    /// A Bounded <see cref="Channel{T}"/> will avoid allocations
    /// when there is only one waiter, and that is our common case. 
    /// </summary>
    public class SemaphoreSuperSlim
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
        public async ValueTask<bool> WaitForNext(CancellationToken token = default)
        {
            return await lockChannel.Reader.WaitToReadAsync(token);
        }

        public bool IsAcquired()
        {
            return lockChannel.Reader.TryPeek(out _);
        }

        /// <summary>
        /// Waits for and acquired a 
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public async ValueTask<bool> WaitAndAcquire(CancellationToken token = default)
        {
            await lockChannel.Writer.WriteAsync(0, token);
            return true;
        }
        
        public async ValueTask<SemaphoreSuperSlimContext> WaitAndAcquireSafe(CancellationToken token = default)
        {
            await lockChannel.Writer.WriteAsync(0, token);
            return new SemaphoreSuperSlimContext(this);
        }
        
        /// <summary>
        /// Tries to set the waiter as no longer waiting.
        /// </summary>
        /// <returns>If -false-, the waiter was already set to not-waiting</returns>
        public bool TryAcquire()
        {
            return lockChannel.Writer.TryWrite(0);
        }
	
        /// <summary>
        /// Tries to set the waiter as unused.
        /// </summary>
        /// <returns>If -false-, the waiter was already set as waiting</returns>
        public bool TryRelease()
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
    /// <summary>
    /// We use this because, unlike <see cref="SemaphoreSlim"/>,
    /// A Bounded <see cref="Channel{T}"/> will avoid allocations
    /// when there is only one waiter, and that is our common case. 
    /// </summary>
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