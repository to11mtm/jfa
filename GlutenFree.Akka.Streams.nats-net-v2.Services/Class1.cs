using System.Threading.Channels;
using Akka;
using Akka.Streams.Dsl;
using NATS.Client.Core;
using NATS.Client.Services;
using static NATS.Client.Services.NatsSvcServer;

namespace GlutenFree.Akka.Streams.nats_net_v2.Services;

public static class DSL
{
    public static Source<HeldEndpointValue<T>, NotUsed> NatsServiceEndpointSource<T>(
        Group serviceContext,
        INatsDeserialize<T>? serializer = null,
        IDictionary<string, string>? metadata = null,
        string? name = null,
        string? subject = null,
        string? queueGroup = null,
        int? maxPending = null, CancellationToken stopToken = default,
        CancellationToken reqToken = default)
    {
        var ch = maxPending == null
            ? Channel.CreateUnbounded<HeldEndpointValue<T>>()
            : Channel.CreateBounded<HeldEndpointValue<T>>(maxPending.Value);
        return Source.UnfoldResourceAsync<HeldEndpointValue<T>,(Channel<HeldEndpointValue<T>> ch,CancellationTokenSource cts)>(async () =>
        {
            var cts =
                CancellationTokenSource.CreateLinkedTokenSource(stopToken);
            var ch = maxPending == null
                ? Channel.CreateUnbounded<HeldEndpointValue<T>>()
                : Channel.CreateBounded<HeldEndpointValue<T>>(maxPending.Value);

            await serviceContext.AddEndpointAsync<T>(async msg =>
            {
                var wrapped = new HeldEndpointValue<T>(msg);
                await ch.Writer.WriteAsync(wrapped,cts.Token);
                await wrapped.GetCompletion;
            }, name, subject, queueGroup, metadata, serializer, cts.Token);
            return (ch, cts);
        }, async uf =>
        {
            var r = await (uf.ch.Reader.ReadAsync(uf.cts.Token));
            return r;
        }, async uf =>
        {
            uf.ch.Writer.TryComplete();
            uf.cts.Cancel();
            return Done.Instance;
        });
    }

    /// <summary>
    /// Signifies an Endpoint Request for Processing. 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public struct HeldEndpointValue<T>
    {
        //TODO: IValueTaskCompletionSource
        private readonly TaskCompletionSource _completionSource;

        internal ValueTask GetCompletion =>
            new ValueTask(_completionSource.Task);

        public HeldEndpointValue(NatsSvcMsg<T> msg)
        {
            Message = msg;
            _completionSource = new TaskCompletionSource(TaskCreationOptions
                .RunContinuationsAsynchronously);
        }

        public NatsSvcMsg<T> Message { get; }

        public void Reply(NatsHeaders? headers = default, string? replyTo = default, NatsPubOpts? opts = default, CancellationToken token = default)
        {
            Message.ReplyAsync(headers, replyTo, opts, token);
        }
        public void MarkCompleted()
        {
            _completionSource.TrySetResult();
        }

        public void MarkFaulted(Exception error)
        {
            _completionSource.TrySetException(error);
        }

        public void TrySetCancelled(CancellationToken token = default)
        {
            _completionSource.TrySetCanceled(token);
        }
    }
}