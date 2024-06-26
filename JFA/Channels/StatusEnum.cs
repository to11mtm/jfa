namespace JFA.Channels;

[Flags]
public enum StatusEnum
{
    NoState =0,
    ReadersWaiting = 2,
    WritersWaiting = 4,
    ReadersReady = 8,
    WritersReady = 16,
    NothingToDoClosed = 32,
}