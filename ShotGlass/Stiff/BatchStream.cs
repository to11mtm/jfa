using System;
using System.Threading.Tasks;
using Akka;
using Akka.Actor;
using Akka.Pattern;
using Akka.Streams.Dsl;
using LanguageExt;

namespace ShotGlass.Stiff
{
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

                            return Akka.Util.Option<((long, BatchFlowControl
                                ),
                                Seq<TElem>)>.Create((
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