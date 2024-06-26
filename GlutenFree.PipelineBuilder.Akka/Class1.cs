using Akka;
using Akka.Streams.Dsl;
using Akka.Util;

namespace GlutenFree.PipelineBuilder.Akka;

public class PipelineRequestBuilder
{
    public static Source<TRequest,NotUsed> StartPipelineFromKnownSet<TRequest>(
        IEnumerable<TRequest> requests)
    {
        return Source.From(requests);
    }
    public static Source<TRequest,NotUsed> StartPipelineFromKnownSet<TRequest>(
        Func<IAsyncEnumerable<TRequest>> requests)
    {
        return Source.From(requests);
    }
}

public class PipelineRequest<TRequest, TResponse>
{
    public TRequest Request { get; }
    public TaskCompletionSource<Try<TResponse>> Response { get; }

    public PipelineRequest(TRequest request)
    {
        Request = request;
        Response =
            new TaskCompletionSource<Try<TResponse>>(TaskCreationOptions
                .RunContinuationsAsynchronously);
    }
}