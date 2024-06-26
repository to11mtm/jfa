namespace JFA.Channels;

public interface IStageToRun
{
    public int MaxToRun { get; }
    public ValueTask RunOne();
}