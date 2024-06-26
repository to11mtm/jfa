using System.Threading.Channels;

namespace JFA.Channels;


public static class ChannelExts
{
    public static async Task WriteAllAsync<T>(
        this ChannelWriter<T> channelWriter, 
        IAsyncEnumerable<T> enumerable,
        bool complete = false,
        CancellationToken token=default
        )
    {
        await foreach (var item in enumerable.WithCancellation(token))
        {
            await channelWriter.WriteAsync(item, token);
        }
        
        if (complete)
        {
            channelWriter.TryComplete();   
        }
    }

    public static async Task WriteAllAsync<T>(
        this ChannelWriter<T> channelWriter, 
        IEnumerable<T> enumerable,
        bool complete = true,
        CancellationToken token = default)
    {
        foreach (var item in enumerable)
        {
            await channelWriter.WriteAsync(item, token);
        }

        if (complete)
        {
            channelWriter.TryComplete();   
        }
    }

    public static async Task WriteWithTransformAsync<TOut, TIn>(
        this ChannelWriter<TOut> channelWriter,
        IAsyncEnumerable<TIn> enumerable,
        Func<TIn, CancellationToken, ValueTask<TOut>> transform,
        bool complete = true,
        CancellationToken token=default
        )
    {
        await foreach (var item in enumerable.WithCancellation(token))
        {
            await channelWriter.WriteAsync(await transform(item, token), token);
        }
        
        if (complete)
        {
            channelWriter.TryComplete();   
        }
    }
    public static async Task WriteWithTransformAsync<TOut, TIn>(
        this ChannelWriter<TOut> channelWriter,
        IEnumerable<TIn> enumerable,
        Func<TIn, CancellationToken, ValueTask<TOut?>> transform,
        bool complete = true,
        CancellationToken token=default
    )
    {
        foreach (var item in enumerable)
        {
            var val = await transform(item, token);
            if (val != null)
            {
                await channelWriter.WriteAsync(val, token);   
            }
        }
        
        if (complete)
        {
            channelWriter.TryComplete();   
        }
    }
}