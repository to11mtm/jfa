using System.Threading.Channels;

namespace JFA.Channels;

public class ChannelPulseBatcher<TIn, TBatch> : ChannelBatcher<TIn, TBatch>
{
    public ChannelPulseBatcher(ChannelReader<TIn> reader,
        ChannelWriter<TBatch> writer, int maxSize, Func<TIn, int> cost,
        Func<TIn, TBatch> seed, Func<TIn, TBatch, TBatch> add,
        int maxBuffer = 1, bool completeWriterOnReadComplete = true) : base(
        reader, writer, maxSize, cost, seed, add, maxBuffer,
        completeWriterOnReadComplete)
    {
    }

    public StatusEnum RunInAndOut()
    {
        var startState = GetState();
        var returnState = StatusEnum.NoState;
        int r = 0;
        int w = 0;
        if (startState.HasFlag(StatusEnum.ReadersReady))
        {
            while (true)
            {
                r = TryDoReads();
                if (r > 0)
                {
                    break;
                }
            }
        }
        if (startState.HasFlag(StatusEnum.WritersReady))
        {
            w = TryFlushFlushNormally();
        }

        if (r == 1)
        {
            returnState = returnState | StatusEnum.ReadersWaiting;
        }

        if (w == 0)
        {
            returnState = returnState | StatusEnum.WritersWaiting;
        }

        return returnState;
    }
}