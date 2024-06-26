using Akka;
using Akka.Streams.Dsl;
using Akka.Util;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATSWrappers;

namespace GlutenFree.Akka.Streams.nats_net_v2;

public static partial class DSL
{
    public static Source<NatsJSMsg<T>, NotUsed> NatsJetStreamFromConnection<T>(
        NatsJSContext context,
        string stream,
        ConsumerConfig config,
        NatsJSConsumeOpts consumeOpts,
        INatsDeserialize<T>? serializer = default, NatsSubOpts? opts = default,
        CancellationToken connectionCancellation = default,
        CancellationToken readCancellation = default)
    {
        return Source.UnfoldResourceAsync<NatsJSMsg<T>,(INatsJSConsumer,IAsyncEnumerator<NatsJSMsg<T>>)>(async () =>
        {
            var consumer = await context.CreateOrUpdateConsumerAsync(stream, config, connectionCancellation);
            var consumerEnumerable = consumer.ConsumeAsync(serializer,consumeOpts,readCancellation);
            return (consumer, consumerEnumerable.GetAsyncEnumerator(readCancellation));
        }, async consumer =>
        {
            var (c, ce) = consumer;
            {
                if (await ce.MoveNextAsync())
                {
                    return  ce.Current;
                }
                else
                {
                    return Option<NatsJSMsg<T>>.None;
                }
            }
        }, async (sub) =>
        {
            try
            {
                await sub.Item2.DisposeAsync();
            }
            catch (Exception e)
            {
                    
            }
            return Done.Instance;
        });
    }

    public static Source<NatsJsWrapper<T>, NotUsed> WrappedJSFromContext<T>(
        NatsJSContext context,
        string stream,
        ConsumerConfig config,
        NatsJSConsumeOpts consumeOpts,
        INatsSerializer<T> serializer = default,
        NatsSubOpts? opts = default,
        CancellationToken connectionCancellation = default,
        CancellationToken readCancellation = default)
    {
        return Source
            .UnfoldResourceAsync<NatsJsWrapper<T>, (INatsJSConsumer,
                IAsyncEnumerator<NatsJSMsg<T>>)>(async () =>
            {
                var consumer = await context.CreateOrUpdateConsumerAsync(stream,
                    config, connectionCancellation);
                var consumerEnumerable = consumer.ConsumeAsync(serializer,
                    consumeOpts, readCancellation);
                return (consumer,
                    consumerEnumerable.GetAsyncEnumerator(readCancellation));
            }, async consumer =>
            {
                var (c, ce) = consumer;
                {
                    if (await ce.MoveNextAsync())
                    {
                        return Option<NatsJsWrapper<T>>.Create(
                            new NatsJsWrapper<T>(ce.Current));
                    }
                    else
                    {
                        return Option<NatsJsWrapper<T>>.None;
                    }
                }
            }, async (sub) => { return Done.Instance; });
    }
}