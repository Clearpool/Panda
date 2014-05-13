package com.clearpool.panda.core;

class Packet implements Comparable<Packet>
{
	private final long sequenceNumber;
	private final byte messageCount;
	private final byte[] bytes;
	
	Packet(long sequenceNumber, byte messageCount, byte[] bytes)
	{
		this.sequenceNumber = sequenceNumber;
		this.messageCount = messageCount;
		this.bytes = bytes;
	}

	long getSequenceNumber()
	{
		return this.sequenceNumber;
	}

	byte getMessageCount()
	{
		return this.messageCount;
	}

	byte[] getBytes()
	{
		return this.bytes;
	}

	@Override
	public int compareTo(Packet o)
	{
		if(this.sequenceNumber < o.sequenceNumber) return -1;
		if(this.sequenceNumber > o.sequenceNumber) return 1;
		return 0;
	}
}