using System.Text;
using Akka;
using Akka.Streams;
using Akka.Streams.Actors;
using Akka.Streams.Dsl;
using Akka.Streams.Implementation.Fusing;
using Akka.Streams.Implementation.Stages;
using Akka.Streams.Stage;
using Akka.Streams.Supervision;
using Akka.Util;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Client.KeyValueStore;
using NATSWrappers;
using ValueTaskSupplement;

namespace GlutenFree.Akka.Streams.nats_net_v2;
   
//TODO: When https://github.com/akkadotnet/akka.net/pull/7028/ merged,
//      revise and use ValueTask Unfolds/SelectAsyncs/etc.
public static partial class DSL
{
    public record NatsSubSourceStageOpts<T>(
        string Subject,
        string? QueueGroup = default,
        INatsDeserialize<T> Deserialize = default,
        NatsSubOpts? Opts = default);
    
    public record NatsPublishStageOpts<T, TPub>(
        Func<T, Option<TPub>> PublishProduce,
        Func<T, string> PublishOver,
        Func<T, NatsHeaders>? HeadersAdd = null,
        Func<T, string>? ReplyTo = null,
        Func<T, INatsSerialize<TPub>>? Serializer = null,
        Func<T, NatsPubOpts>? Opts = null);
    
    /// <summary>
    /// Convenience Method to convert a general publish stage option set into one with output. 
    /// </summary>
    public static NatsPublishStageOpts<T, TPub, TOut> WithOutput<T, TPub, TOut>(
        this NatsPublishStageOpts<T, TPub> pub,
        Func<T, TOut> ignore,
        Func<T, TPub, TOut> success,
        Func<T, TPub, Exception, TOut> failure)
    {
        return new NatsPublishStageOpts<T, TPub, TOut>(pub.PublishProduce,
            pub.PublishOver, ignore, success, failure, pub.HeadersAdd,
            pub.ReplyTo, pub.Serializer, pub.Opts);
    }
    
    //Different name when it is a pass-through?
    public record NatsPublishStageOpts<T, TPub, TOut>(
        Func<T, Option<TPub>> PublishProduce,
        Func<T, string> PublishOver,
        Func<T, TOut> Ignore,
        Func<T, TPub, TOut> Success,
        Func<T, TPub, Exception, TOut> Failure,
        Func<T, NatsHeaders>? HeadersAdd = null,
        Func<T, string>? ReplyTo = null,
        Func<T, INatsSerialize<TPub>>? Serializer = null,
        Func<T, NatsPubOpts>? Opts = null) : NatsPublishStageOpts<T, TPub>(
        PublishProduce, PublishOver, HeadersAdd, ReplyTo, Serializer, Opts);
    
    /// <summary>
    /// Creates a Subscription source based on a Nats connection and provided options.
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="stageOpts"></param>
    /// <param name="connectionCancellation"></param>
    /// <param name="readCancellation"></param>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public static Source<NatsMsg<T>,NotUsed> NatsSubscribeSource<T>(
        this NatsConnection connection,
        NatsSubSourceStageOpts<T> stageOpts,
        CancellationToken connectionCancellation = default,
        CancellationToken readCancellation = default
        )
    {
        return Source.UnfoldResourceAsync(async () =>
            {
                return await connection.SubscribeCoreAsync<T>(stageOpts.Subject,
                    stageOpts.QueueGroup, stageOpts.Deserialize, stageOpts.Opts, connectionCancellation);
            },
            readCancellation == default? funcHelpers<T>.func : Read, async (sub) =>
            {
                await sub.DisposeAsync();
                return Done.Instance;
            });
        
        async Task<Option<NatsMsg<T>>> Read(INatsSub<T> a)
        {
            do
            {
                if (a.Msgs.TryRead(out var item))
                {
                    return item;
                }
            } while (await a.Msgs.WaitToReadAsync(readCancellation));

            return Option<NatsMsg<T>>.None;
        }
    }

    /// <summary>
    /// A publish stage that takes an input, decides whether to publish,
    /// And After publishing (or not) produces a new message to downstream,
    /// Based on provided options.
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="parallelism">
    /// Max number of publishes to run in parallel.
    /// Set to 1 for guaranteed ordering.
    /// </param>
    /// <param name="stageOpts"></param>
    /// <param name="token"></param>
    /// <typeparam name="TIn"></typeparam>
    /// <typeparam name="TPub"></typeparam>
    /// <typeparam name="TOut"></typeparam>
    /// <returns></returns>
    public static Flow<TIn, TOut, NotUsed> NatsPublishStage<TIn,TPub, TOut>(
        this NatsConnection connection,
        int parallelism,
        NatsPublishStageOpts<TIn,TPub,TOut> stageOpts,
        CancellationToken token)
    {
        return Flow.FromGraph(
            new SelectAsync<TIn, TOut>(1, async (t) =>
            {
                var p = stageOpts.PublishProduce(t);
                if (p.HasValue)
                {
                    var v = p.Value;
                    try
                    {
                        await connection.PublishAsync(
                            stageOpts.PublishOver(t), v,
                            stageOpts.HeadersAdd?.Invoke(t),
                            stageOpts.ReplyTo?.Invoke(t),
                            stageOpts.Serializer?.Invoke(t),
                            stageOpts.Opts?.Invoke(t),
                            token);
                        return stageOpts.Success(t, v);
                    }
                    catch (Exception e)
                    {
                        return stageOpts.Failure(t, v, e);
                    }
                }
                else
                {
                    return stageOpts.Ignore(t);
                }
            }));
    }
    
    /// <summary>
    /// Sends a series of Elements to be published based on the Option Transform.
    /// May optionally run in parallelism with optional 'finish ordering'.
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="parallelism">Number of publishes to run in parallel. </param>
    /// <param name="stageOpts"></param>
    /// <param name="requireFinishOrder">If true and <see cref="parallelism"/> is greater than 1, freeing of publishers is based on the when the oldest publish is completed</param>
    /// <param name="token"></param>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="TPub"></typeparam>
    /// <returns>A sink that may be attached to a stream and run.</returns>
    /// <exception cref="Exception"></exception>
    public static Sink<T, Task<Done>> NatsPublishSink<T,TPub>(
        this NatsConnection connection,
        int parallelism,
        NatsPublishStageOpts<T,TPub> stageOpts,
        bool requireFinishOrder,
        CancellationToken token = default)
    {
        if (requireFinishOrder)
        {
            return NatsPublishStage(connection, parallelism,
                    stageOpts.WithOutput(a => NotUsed.Instance,
                        (a, b) => NotUsed.Instance, (a, b, c) => throw c), token)
                .ToMaterialized(Sink.Ignore<NotUsed>(),Keep.Right);
        }
        else
        {
            return Sink.ForEachAsync<T>(parallelism, async (a) =>
            {
                var r = stageOpts.PublishProduce(a);
                if (r.HasValue)
                {
                    await connection.PublishAsync<TPub>(
                        stageOpts.PublishOver(a),
                        r.Value,
                        stageOpts.HeadersAdd?.Invoke(a),
                        stageOpts.ReplyTo?.Invoke(a),
                        stageOpts.Serializer?.Invoke(a),
                        stageOpts.Opts?.Invoke(a));
                }
            });
        }
    }
}

/// <remarks>
/// Used to minimize allocation for default case 
/// </remarks>
internal static class funcHelpers<T>
{
    internal static readonly Func<INatsSub<T>, Task<Option<NatsMsg<T>>>> func = async a=>{
        do
        {
            if (a.Msgs.TryRead(out var item))
            {
                return item;
            }
        } while (await a.Msgs.WaitToReadAsync());
        return Option<NatsMsg<T>>.None;
    };
    
}
