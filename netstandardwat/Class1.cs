using System;

namespace netstandardwat
{
    public class Class1
    {
        public bool contains(Memory<byte> m, Memory<byte> other)
        {
            m.Span.IndexOf(other.Span)
        }
    }
}