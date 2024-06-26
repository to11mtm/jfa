namespace GlutenFree.Composition;

public sealed class TriState
{
    public sealed class TriStateCompletedException : Exception
    {
        public static readonly TriStateCompletedException Instance = new();
    }

}