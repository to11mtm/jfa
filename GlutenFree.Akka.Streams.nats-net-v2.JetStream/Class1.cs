using System.Text;
using System.Threading.Tasks.Dataflow;
using Akka;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Implementation.Fusing;
using Akka.Util;
using JetBrains.Annotations;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace GlutenFree.Akka.Streams.nats_net_v2.JetStream;


public static class DSL
{
    public static Source<NatsJSMsg<T>, NotUsed> NatsJsOrderedConsumerSource<T>(NatsJSContext context,
        string stream,
        NatsJSOrderedConsumerOpts? opts = default,
        NatsJSConsumeOpts? consumeOpts = default,
        INatsDeserialize<T>? consumeSerializer = default,
        CancellationToken token = default,
        CancellationToken consumeCancel = default)
    {
        return Source.UnfoldResourceAsync([MustDisposeResource]async () =>
            {
                return (await context.CreateOrderedConsumerAsync(stream, opts,
                    token)).ConsumeAsync(consumeSerializer, consumeOpts,
                    consumeCancel).GetAsyncEnumerator(consumeCancel);
            },
            async (jsc) =>
            {
                if (await jsc.MoveNextAsync())
                {
                    return jsc.Current;
                }
                else
                {
                    return Option<NatsJSMsg<T>>.None;
                }
            }, async (jsc) =>
            {
                    await jsc.DisposeAsync();
                    return Done.Instance;
            });
    }

    public static Sink<T, Task<Done>> NatsJsPublishSink<T, TPub>(NatsJSContext context,
        Func<T, string> subject, Func<T, TPub> pubTransform,
        INatsSerializer<TPub>? serializer = default,
        NatsJSPubOpts? opts = default,
        NatsHeaders? headers = default,
        CancellationToken cancellationToken = default)
    {
        return Sink.ForEachAsync<T>(1, async t =>
        {
            await context.PublishAsync(subject(t), pubTransform(t), serializer, opts,
                headers, cancellationToken);
        });
    }

    public record WithPublishOpts<T, TPub, TOut>(
        Func<T, Option<TPub>> Publish,
        Func<TPub, string> PublishOver,
        Func<T, TPub, PubAckResponse, TOut> Success,
        Func<T, TPub, Exception, TOut> Failure,
        Func<T, TOut> Ignore,
        Func<TPub, INatsSerializer<TPub>?>? PubSer = default,
        Func<TPub, NatsJSPubOpts?>? PubOpts = default,
        Func<TPub, NatsHeaders?>? Headers = default);

    public static Flow<T, TOut, NotUsed> NatsJSPublishStage<T, TPub, TOut>(
        NatsJSContext context, WithPublishOpts<T, TPub, TOut> opts,
        CancellationToken token = default)
    {
        return Flow.FromGraph(
            new SelectAsync<T, TOut>(1, async (t) =>
            {
                var p = opts.Publish(t);
                if (p.HasValue)
                {
                    var v = p.Value;
                    try
                    {
                        var resp = await context.PublishAsync(
                            opts.PublishOver(v), v,
                            opts.PubSer?.Invoke(v),
                            opts.PubOpts?.Invoke(v), opts.Headers?.Invoke(v),
                            token);
                        return opts.Success(t, v, resp);
                    }
                    catch (Exception e)
                    {
                        return opts.Failure(t, v, e);
                    }
                }
                else
                {
                    return opts.Ignore(t);
                }
            }));
    }
    public static IFlow<TOut, TMat> WithNatsJsPublish<T, TPub, TOut, TMat>(
        this IFlow<T, TMat> flow,
        NatsJSContext context,
        WithPublishOpts<T,TPub,TOut> opts,
        CancellationToken token = default)
    {
        return flow.Via(NatsJSPublishStage<T,TPub,TOut>(context,opts,token));
    }
    public static Sink<T, Task<Done>> NatsJSOutputSink<T, TPub>(NatsJSContext context,
        WithPublishOpts<T,TPub,NotUsed> opts,
        CancellationToken cancellationToken = default)
    {
        return Sink.ForEachAsync<T>(1,
            async (t) =>
            {
                var p = opts.Publish(t);
                if (p.HasValue)
                {
                    var v = p.Value;
                    try
                    {
                        var resp = await context.PublishAsync(
                            opts.PublishOver(v), v,
                            opts.PubSer?.Invoke(v),
                            opts.PubOpts?.Invoke(v), opts.Headers?.Invoke(v),
                            cancellationToken);
                        opts.Success(t, v, resp);
                    }
                    catch (Exception e)
                    {
                        throw;
                    }
                }
                else
                {
                    opts.Ignore(t);
                }
            });
    }
     
}

public record NatsJSCarrier<TNats, TOther>(
    NatsJSMsg<TNats> NatsJsMsg,
    TOther Other);

public static class NatsJSCarrier
{
    public static NatsJSCarrier<TNats, TOther> ToCarrier<TNats, TOther>(
        this NatsJSMsg<TNats> natsJsMsg, TOther otherMsg)
    {
        return new NatsJSCarrier<TNats, TOther>(natsJsMsg, otherMsg);
    }

    public static NatsJSCarrier<TNats, TOther>
        ToCarrier<TNats, TOtherOld, TOther>(
            this NatsJSCarrier<TNats, TOtherOld> natsJsCarrier, TOther other)
    {
        return new NatsJSCarrier<TNats, TOther>(natsJsCarrier.NatsJsMsg, other);
    }
}