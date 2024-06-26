using System.Buffers;
using System.Collections;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace JFA.Channels;

public class ULLBoundedChannel<T> : Channel<T>
{
    private readonly int _capacity;
    private readonly object _mainLock = new();
    private readonly UnrolledLinkedList<T> _list = new UnrolledLinkedList<T>();
    public ULLBoundedChannel(int capacity)
    {
        _capacity = capacity;
    }
    private sealed class ULLBoundedChannelWriter : ChannelWriter<T>
    {
        private readonly ULLBoundedChannel<T> _parent;

        public ULLBoundedChannelWriter(ULLBoundedChannel<T> parent)
        {
            _parent = parent;
        }
        public override bool TryWrite(T item)
        {
            throw new NotImplementedException();
        }

        public override ValueTask<bool> WaitToWriteAsync(CancellationToken cancellationToken = new CancellationToken())
        {
            throw new NotImplementedException();
        }
    }
    private sealed class ULLBoundedChannelReader : ChannelReader<T>
    {
        public override bool TryRead(out T item)
        {
            throw new NotImplementedException();
        }

        public override ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken = new CancellationToken())
        {
            throw new NotImplementedException();
        }
    }
}

//USE AT OWN RISK, PORTED UNTESTED CODE

    /// <devremarks>
    /// vaguely based on
    /// https://github.com/l-tamas/Unrolled-linked-list/blob/master/src/org/megatherion/util/collections/UnrolledLinkedList.java
    /// </devremarks>
    public class UnrolledLinkedList<T>
        : IEnumerable<T>
    {
        private readonly int nodeCapacity = Node.NodeSize;
        private int size;
        private int modCount;
        private Node firstNode;
        private Node lastNode;
	
        public UnrolledLinkedList()
        {
            System.Buffers.
            firstNode = new Node();
            lastNode = firstNode;
        }
        public T this[int index]
        {
            get
            {
                var node = _findnode(index, out var p);
                return node.Elements[index - p];
            }
            set
            {
                T ret = default;
                var node = _findnode(index, out var p);
                ret = node.Elements[index - p];
                node.Elements[index - p] = value;
            }
        }
		
        private Node _findnode(int index, out int p)
        {
            Node node;
            p = 0;
            if (size - index > index)
            {
                node = firstNode;
                while (p <= index - node.numElements)
                {
                    p += node.numElements;
                    node = node.next;
                }
            }
            else
            {
                node = lastNode;
                p = size;
                while ((p -= node.numElements) > index)
                {
                    node = node.previous;
                }
            }
            return node;
        }

        public int Count => size;

        public bool IsReadOnly => false;

        public void Add(T item)
        {
            insertIntoNode(lastNode,lastNode.numElements,item);
        }

        public void Clear()
        {
            Node node = firstNode.next;
            while (node != null)
            {
                //TODO: Pool these somehow.
                Node next = node.next;
                node.next = null;
                node.previous = null;
                //Because the array is readonly, we clear it here.
                //JVM nulled the array out but we have it as readonly,
                //since readonly with known size set is better for JIT
                //(IMO, anyway)
                Array.Clear(node.Elements,0,node.numElements);
                node.numElements = 0;
                node = next;
            }
            lastNode = firstNode;
            Array.Clear(firstNode.Elements,0,firstNode.numElements);
            firstNode.numElements = 0;
            firstNode.next = null;
            size = 0;
        }

        public bool Contains(T item)
        {
            return IndexOf(item)>=0;
        }

        public void CopyTo(T[] array, int arrayIndex)
        {
            if (array== null || array.Length+arrayIndex <size)
            {
                throw new ArgumentException("Array+startIndex is smaller than source!");
            }
            for (Node node = firstNode; node != null; node = node.next)
            {
                node.CurrentElements.CopyTo(array.AsMemory(arrayIndex,node.numElements));
                arrayIndex += node.numElements;
			
            }
        }
	
        public IEnumerator<T> GetEnumerator()
        {
            return new ULLEnumerator(this);
        }

        public T PopFirst()
        {
            if (size < 1)
            {
                throw new InvalidOperationException("Nothing to pop!");
            }
            var item = firstNode.Elements[0];
            removeFromNode(firstNode,0);
            return item;
        }

        public void AddLast(T item)
        {
            Add(item);
        }
        
        
        public int IndexOf(T item)
        {
            int index = 0;
            Node node = firstNode;
            if (item == null)
            {
                while (node != null)
                {
                    for (int i=0; i<node.numElements; i++)
                    {
                        if (node.Elements[i] == null)
                        {
                            return index + i;
                        }
                    }
                    index += node.numElements;
                    node = node.next;
                }
            }
            else
            {
                while (node != null)
                {
                    for (int ptr = 0; ptr<node.numElements; ptr++)
                    {
                        if (item.Equals(node.Elements[ptr]))
                        {
                            return index + ptr;
                        }
                    }
                    index += node.numElements;
                    node = node.next;
                }
            }
            return -1;
        }

        public T Head => this[0];
	
        private void removeFromNode(Node node, int ptr)
        {
            node.numElements--;
            for (int i=ptr; i<node.numElements;i++)
            {
                node.Elements[i] = node.Elements[i+1];
            }
            node.Elements[node.numElements] = default;
            if (node.next != null && node.next.numElements + node.numElements <= nodeCapacity)
            {
                mergeWithNextNode(node);
            }
            else if (node.previous != null && node.previous.numElements + node.numElements <= nodeCapacity)
            {
                mergeWithNextNode(node.previous);
            }
            size--;
            modCount++;
        }
	
        private void mergeWithNextNode(Node node)
        {
            Node next = node.next;
            for (int i=0; i<next.numElements; i++)
            {
                node.Elements[node.numElements+i] = next.Elements[i];
                next.Elements[i] = default;
            }
            node.numElements += next.numElements;
            if (next.next != null)
            {
                next.next.previous = node;
            }
            node.next = next.next;
            if (next == lastNode){
                lastNode = node;
            }
        }
        private void insertIntoNode(Node node, int ptr, T element)
        {
            if (node.numElements == nodeCapacity) {
                Node newNode = new Node();
		//TODO: May not be safe with capacity not divisible by 2.
                int elementsToMove = nodeCapacity/2;
                int startIndex = nodeCapacity - elementsToMove;
                int i;
                for (i=0; i<elementsToMove; i++)
                {
                    newNode.Elements[i] = node.Elements[startIndex+i];
                    node.Elements[startIndex+i]= default;
                }
                node.numElements -= elementsToMove;
                newNode.numElements = elementsToMove;
                newNode.next = node.next;
                newNode.previous = node;
                if (node.next != null)
                {
                    node.next.previous = newNode;
                }
                node.next = newNode;
                if (node == lastNode)
                {
                    lastNode = newNode;
                }
                if (ptr > node.numElements)
                {
                    node = newNode;
                    ptr -= node.numElements;
                }
            }
            for (int i=node.numElements; i>ptr; i--)
            {
                node.Elements[i] = node.Elements[i-1];
            }
            node.Elements[ptr] = element;
            node.numElements++;
            size++;
            modCount++;
        }

        public void AddFirst(T item)
        {
            insertIntoNode(firstNode,0,item);
        }
        public void Insert(int index, T item)
        {
            if (index<0 || index > size)
            {
                throw new IndexOutOfRangeException();
            }
            var node = _findnode(index,out var p);
            insertIntoNode(node, index-p, item);
        }

        public bool Remove(T item)
        {
            int idx = 0;
            var node = firstNode;
            if (item == null){
                while(node != null)
                {
                    for (int ptr = 0; ptr<node.numElements; ptr++)
                    {
                        if (node.Elements[ptr] == null)
                        {
                            removeFromNode(node,ptr);
                            return true;
                        }
                    }
                    idx += node.numElements;
                    node = node.next;
                }
            }
            else
            {
                while (node != null)
                {
                    for (int ptr = 0; ptr<node.numElements; ptr++)
                    {
                        if(item.Equals(node.Elements[ptr]))
                        {
                            removeFromNode(node,ptr);
                            return true;
                        }
                    }
                    idx += node.numElements;
                    node = node.next;
                }
            }
            return false;
        }

        public void RemoveAt(int index)
        {
            if (index <0 || index>= size)
            {
                throw new IndexOutOfRangeException();
            }
            T element = default;
            Node node;
            int p = 0;
            if (size-index > index)
            {
                node = firstNode;
                while (p<= index-node.numElements)
                {
                    p += node.numElements;
                    node = node.next;
                }
            }
            else
            {
                node = lastNode;
                p = size;
                while ((p-= node.numElements)>index)
                {
                    node = node.previous;
                }
            }
            element = node.Elements[index-p];
            removeFromNode(node,index-p);
		
        }

        IEnumerator IEnumerable.GetEnumerator()=>GetEnumerator();

        public class Node
        {
            private static int GetNodeSize()
            {
                if (typeof(T).IsValueType)
                {
                    return Math.Max(8, (120 / Unsafe.SizeOf<T>()) & (Int32.MaxValue-1));    
                }
                else
                {
                    return 32;
                }
            }
            public Memory<T> CurrentElements => Elements.AsMemory(0,numElements);
            public static readonly int NodeSize = GetNodeSize();
            public Node next;
            public Node previous;
            public int numElements = 0;
            public readonly T[] Elements = new T[NodeSize];
        }

        private class ULLEnumerator : IEnumerator<T>
        {
            readonly UnrolledLinkedList<T> list;
            Node currentNode;
            int ptr;
            int index;
            T current;
            public ULLEnumerator(UnrolledLinkedList<T> list)
            {
                this.list = list;
                currentNode = list.firstNode;
            }
            public T Current => current;

            object IEnumerator.Current => current;

            public void Dispose()
            {
			
            }

            public bool MoveNext()
            {
                if (hasNext()==false)
                {
                    return false;
                }
                if (ptr>= currentNode.numElements)
                {
                    if (currentNode.next != null) {
                        currentNode = currentNode.next;
                        ptr = 0;
                    }
                }
                current = currentNode.Elements[ptr];
                ptr++;
                index++;
                return true;
            }
		
            public bool hasNext()
            {
                return (index<=list.size-1);
            }

            public void Reset()
            {
                index = 0;
                ptr = 0;
                current = default;
                currentNode = list.firstNode;
            }
        }
    }