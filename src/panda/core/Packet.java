package panda.core;

class Packet implements Comparable<Packet>
{
	private final long sequenceNumber;
	private final byte messageCount;
	private final byte[] bytes;
	
	public Packet(long sequenceNumber, byte messageCount, byte[] bytes)
	{
		this.sequenceNumber = sequenceNumber;
		this.messageCount = messageCount;
		this.bytes = bytes;
	}

	public long getSequenceNumber()
	{
		return this.sequenceNumber;
	}

	public byte getMessageCount()
	{
		return this.messageCount;
	}

	public byte[] getBytes()
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