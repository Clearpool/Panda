package com.clearpool.panda.core;

import java.util.LinkedList;
import java.util.List;

class PacketCache
{
	private final int cacheSize;
	private final byte[][] cache;
	
	private int elementCount;
	private int headIndex;
	private int tailIndex;
	private long headSequenceNumber;
	private long tailSequenceNumber;

	public PacketCache(int cacheSize)
	{
		this.cacheSize = cacheSize;
		this.cache = new byte[this.cacheSize][];

		this.elementCount = 0;
		this.headIndex = 0;
		this.tailIndex = 0;
		this.headSequenceNumber = 0;
		this.tailSequenceNumber = 0;
	}

	public void add(byte[] packetBytes, long sequenceNumber)
	{
		if (this.elementCount == this.cacheSize)
		{
			this.cache[this.headIndex] = packetBytes;
			this.headIndex = getIndex(this.headIndex + 1);
			this.tailIndex = getIndex(this.tailIndex + 1);
			this.headSequenceNumber = (sequenceNumber - this.cacheSize + 1L);
		}
		else if (this.elementCount++ == 0)
		{
			this.cache[this.tailIndex] = packetBytes;
			this.headSequenceNumber = sequenceNumber;
		}
		else
		{
			int nextIndex = getIndex(this.tailIndex + 1);
			this.cache[nextIndex] = packetBytes;
			this.tailIndex = nextIndex;
		}

		this.tailSequenceNumber = sequenceNumber;
	}

	private int getIndex(int index)
	{
		if (index < 0)
			return index + this.cacheSize;
		return index % this.cacheSize;
	}

	public Pair<List<byte[]>, Long> getCachedPackets(long firstSequenceNumberRequested, long lastSequenceNumberRequested)
	{
		if (firstSequenceNumberRequested == 0L)
			return null;
		if (lastSequenceNumberRequested == 0L)
			return null;

		int distanceFirstFromHead = (int) (firstSequenceNumberRequested - this.headSequenceNumber);
		int distanceLastFromTail = (int) (this.tailSequenceNumber - lastSequenceNumberRequested);

		if ((lastSequenceNumberRequested < this.headSequenceNumber) || (firstSequenceNumberRequested > this.tailSequenceNumber))
			return null;
		if (distanceFirstFromHead >= this.elementCount)
			return null;
		if (distanceLastFromTail >= this.elementCount)
			return null;
		int firstIndex = getIndex(this.headIndex + (distanceFirstFromHead < 0 ? 0 : distanceFirstFromHead));
		int lastIndex = getIndex(this.tailIndex - (distanceLastFromTail < 0 ? 0 : distanceLastFromTail));

		List<byte[]> packets = new LinkedList<byte[]>();

		if (lastIndex < firstIndex)
		{
			for (int i = firstIndex; i < this.cacheSize; i++)
			{
				byte[] packet = this.cache[i];
				packets.add(packet);
			}

			for (int i = 0; i <= lastIndex; i++)
			{
				byte[] packet = this.cache[i];
				packets.add(packet);
			}

		}
		else
		{
			for (int i = firstIndex; i <= lastIndex; i++)
			{
				byte[] packet = this.cache[i];
				packets.add(packet);
			}
		}
		long firstPacketSequenceNumber = getSequenceNumberOfIndex(firstIndex);
		return new Pair<List<byte[]>, Long>(packets, Long.valueOf(firstPacketSequenceNumber));
	}

	long getSequenceNumberOfIndex(int index)
	{
		if ((this.elementCount < this.cacheSize) && (index > this.elementCount - 1))
			return 0L;
		if (index == this.headIndex)
			return this.headSequenceNumber;
		if (index == this.tailIndex)
			return this.tailSequenceNumber;

		int distanceFromHead = index - this.headIndex;
		if (distanceFromHead < 0)
			return this.headSequenceNumber + this.cacheSize + distanceFromHead;
		return this.headSequenceNumber + distanceFromHead;
	}
}
