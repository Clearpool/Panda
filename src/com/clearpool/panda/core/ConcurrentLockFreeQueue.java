package com.clearpool.panda.core;

import java.util.concurrent.atomic.AtomicReference;

/**
 * This data structure is based off http://www.research.ibm.com/people/m/michael/podc-1996.pdf
 * 
 * @param <T>
 */
public class ConcurrentLockFreeQueue<T>
{
	private AtomicReference<Node> head;
	private AtomicReference<Node> tail;

	public ConcurrentLockFreeQueue()
	{
		Node dummyNode = new Node(null);
		this.head = new AtomicReference<Node>(dummyNode);
		this.tail = new AtomicReference<Node>(dummyNode);
	}

	public void offer(T value)
	{
		// try to allocate new node from local pool
		Node node = new Node(value);
		while (true)
		{
			// keep trying
			Node last = this.tail.get(); // read tail
			Node next = last.next.get(); // read next
			// are they consistent?
			if (last == this.tail.get())
			{
				if (next == null)
				{
					// was tail the last node?
					// try to link node to end of list
					if (last.next.compareAndSet(next, node))
					{
						// enq done, try to advance tail
						this.tail.compareAndSet(last, node);
						return;
					}
				}
				else
				{
					/*
					 * tail was not the last node try to swing tail to next node
					 */
					this.tail.compareAndSet(last, next);
				}
			}
		}
	}

	public T take()
	{
		while (true)
		{
			Node first = this.head.get();
			Node last = this.tail.get();
			Node next = first.next.get();
			// are they consistent?
			if (first == this.head.get())
			{
				if (first == last)
				{
					// is queue empty or tail falling behind?
					if (next == null)
					{
						// is queue empty?
						return null;
					}
					// tail is behind, try to advance
					this.tail.compareAndSet(last, next);
				}
				else
				{
					T value = next.value;
					// read value before dequeuing
					if (this.head.compareAndSet(first, next))
					{
						return value;
					}
				}
			}
		}
	}

	public class Node
	{
		public T value;
		public AtomicReference<Node> next;

		public Node(T value)
		{
			this.next = new AtomicReference<Node>(null);
			this.value = value;
		}
	}
}
