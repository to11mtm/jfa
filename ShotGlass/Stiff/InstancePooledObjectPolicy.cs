using System;
using System.Collections.Generic;
using GlutenFree.ShotGlass.Stiff;
using Microsoft.Extensions.ObjectPool;

namespace ShotGlass.Stiff
{
    public class InstancePooledObjectPolicy<T> : IPooledObjectPolicy<List<T>> where T: TypedKeyedSequencedRow
    {
        public readonly int MaxSize;

        public InstancePooledObjectPolicy(int maxSize)
        {
            MaxSize = maxSize;
            CreateSize = Math.Max(32, Math.Min(maxSize / 4, 64));
        }

        public readonly int CreateSize;

        public List<T> Create()
        {
            return new List<T>(CreateSize);
        }

        public bool Return(List<T> obj)
        {
            var retVal = obj.Count <= MaxSize;
            obj.Clear();
            return retVal;
        }
    }
}