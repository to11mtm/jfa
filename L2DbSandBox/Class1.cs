using System;
using System.Linq;
using System.Threading.Tasks;
using LanguageExt;
using LanguageExt.Common;
using LinqToDB;
using LinqToDB.Data;
using LinqToDB.DataProvider.PostgreSQL;

namespace L2DbSandBox
{

    public class Straw
    {
        public int Id { get; set; }
        public string Name { get; set; }
    }
    /// <summary>
    /// Defines a basic Structure for Safe repository patterns.
    ///
    /// The Init Task, is intended to be overridden,
    /// If one wishes to use Init logic to ensure those actions run,
    ///   before any DB Operations.
    ///
    /// The Protected Methods, are intended to be used by implementations,
    /// i.e. convenience methods for specific repositories,
    ///  rather than a 'generic' repository implementation.
    /// </summary>
    /// <typeparam name="TDc"></typeparam>
    /// <remarks>
    /// Generic repositories tend to leak on at least one side or the other.
    ///
    /// By instead providing a base abstraction for working with a DB Layer,
    /// We may instead provide a more 'storage agnostic' way to implement.
    /// This also benefits testability; we may use a 'mock' DB and 'pre-fill'
    /// instead of using brittle or less accurate in-memory mocks,
    /// which gives a huge advantage in repro cases.
    /// </remarks>
    public abstract class BaseRepository<TDc> where TDc : DataConnection
    {
        protected readonly IDataConnectionFactory<TDc> CtxFactory;
        protected virtual Task Init { get; } = Task.CompletedTask;

        public BaseRepository(IDataConnectionFactory<TDc> connectionFactory)
        {
            this.CtxFactory = connectionFactory;
        }

        protected async ValueTask UseOrGet(
            Func<TDc, Task> dbAction,
            TDc dataConnection = null)
        {
            await UseOrGet(dbAction,
                async static (db, st) =>
                {
                    await st(db);
                    return Unit.Default;
                },
                dataConnection);
        }
        
        protected async ValueTask<TResult> UseOrGet<TState,TResult>(
            TState state,
            Func<TDc, TState, Task<TResult>> dbAction,
            TDc dataConnection = null)
        {
            if (Init.Status != TaskStatus.RanToCompletion)
            {
                await Init;
            }

            if (dataConnection != null)
            {
                return await dbAction(dataConnection,state);
            }
            else
            {
                await using (var ctx = CtxFactory.GetConnection())
                {
                    return await dbAction(ctx,state);
                }
            }
        }

        protected async ValueTask<TResult> UseOrGet<TResult>(
            Func<TDc, Task<TResult>> dbAction,
            TDc dataConnection = null)
        {
            return await UseOrGet(dbAction,
                static async (dc, st) => await st(dc),
                dataConnection
            );
        }

        protected async ValueTask UseOrGet<TState>(
            TState state,
            Func<TDc, TState, Task> dbAction,
            TDc dataConnection = null)
        {
            await UseOrGet((state, dbAction), async static (dc, db) =>
            {
                await db.dbAction(dc, db.state);
                return Unit.Default;
            }, dataConnection);
        }

        public async ValueTask Transactionally(
            Func<TDc, Task> dbAction,
            TDc dataConnection = null)
        {
            await Transactionally(dbAction,static async (dc,state) =>
            {
                await state(dc);
                return Unit.Default;
            });
        }

        public async ValueTask<T> Transactionally<T>(
            Func<TDc, Task<T>> dbAction,
            TDc dataConnection = null)
        {
            return await Transactionally(dbAction,
                static async (dc, state) => await state(dc),
                dataConnection
                );
        }

        public async ValueTask<T> Transactionally<TState,T>(
            TState state,
            Func<TDc, TState, Task<T>> dbAction,
            TDc dataConnection = null)
        {
            if (dataConnection != null)
            {
                if (dataConnection.Transaction != null)
                {
                    return await dbAction(dataConnection,state);
                }
                else
                {
                    using (var tx =
                           await dataConnection.BeginTransactionAsync())
                    {
                        try
                        {
                            var res = await dbAction(dataConnection,state);
                            await tx.CommitAsync();
                            return res;
                        }
                        catch (Exception e)
                        {
                            Exception ex = e;
                            try
                            {
                                await tx.RollbackAsync();
                            }
                            catch (Exception exception)
                            {
                                ex = new AggregateException(exception, ex);
                            }

                            if (ex == e)
                            {
                                throw;
                            }
                            else
                            {
                                throw ex;
                            }
                        }
                    }
                }
            }
            else
            {
                using (var ctx = CtxFactory.GetConnection())
                {
                    using (var tx = await ctx.BeginTransactionAsync())
                    {
                        try
                        {
                            var res = await dbAction(ctx,state);
                            await tx.CommitAsync();
                            return res;
                        }
                        catch (Exception e)
                        {
                            Exception ex = e;
                            try
                            {
                                await tx.RollbackAsync();
                            }
                            catch (Exception exception)
                            {
                                ex = new AggregateException(exception, ex);
                            }

                            if (ex == e)
                            {
                                throw;
                            }
                            else
                            {
                                throw ex;
                            }
                        }
                    }
                }
            }
        }
        
    }

    public interface IDataConnectionFactory<out TDc> where TDc : DataConnection
    {
        TDc GetConnection();
    }
}