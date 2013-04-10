package panda.core;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.ArrayDeque;
import java.util.List;

class ChannelSendInfo implements SelectorActionable
{
	private final InetAddress multicastIp;
	private final int multicastPort;
	private final String multicastGroup;
	private final InetSocketAddress multicastGroupAddress;
	private final NetworkInterface networkInterface;
	private final ArrayDeque<Integer> topicIdQueue;
	private final ArrayDeque<byte[]> messageQueue;
	private final int cacheSize;
	private final PacketCache packetCache;
	private final byte supportsRetransmissions;

	private DatagramChannel channel;
	private long sequenceNumber;

	public ChannelSendInfo(String ip, int port, String multicastGroup, int cacheSize, String interfaceIp) throws Exception
	{
		this.multicastIp = InetAddress.getByName(ip);
		this.multicastPort = port;
		this.multicastGroup = multicastGroup;
		this.multicastGroupAddress = new InetSocketAddress(this.multicastIp, this.multicastPort);
		this.networkInterface = NetworkInterface.getByInetAddress(InetAddress.getByName(interfaceIp));
		this.topicIdQueue = new ArrayDeque<Integer>();
		this.messageQueue = new ArrayDeque<byte[]>();
		this.cacheSize = cacheSize;
		this.packetCache = (this.cacheSize > 0 ? new PacketCache(this.cacheSize) : null);
		this.supportsRetransmissions = ((byte) (this.cacheSize > 0 ? 1 : 0));

		this.channel = null;
		this.sequenceNumber = 0;
	}

	// Called by app thread
	public boolean addMessageToSendQueue(Integer topicId, byte[] bytes)
	{
		if (bytes.length <= Utils.MAX_MESSAGE_PAYLOAD_SIZE)
		{
			this.topicIdQueue.add(topicId);
			this.messageQueue.add(bytes);
			return true;
		}
		return false;
	}

	// Called by selectorThread
	public boolean hasOutboundDataRemaining()
	{
		return this.messageQueue.size() > 0;
	}

	// Called by selectorThread
	public byte[] getNextPacket()
	{
		if (this.messageQueue.size() == 1)
		{
			Integer topicId = this.topicIdQueue.remove();
			byte[] bytes = this.messageQueue.remove();

			byte messageCount = 1;
			byte[] prependedBytes = new byte[Utils.PACKET_HEADER_SIZE + Utils.MESSAGE_HEADER_SIZE + bytes.length];
			ByteBuffer buffer = ByteBuffer.wrap(prependedBytes);
			buffer.put(Utils.PACKET_HEADER_SIZE);
			buffer.put(this.supportsRetransmissions);
			buffer.putLong(++this.sequenceNumber);
			buffer.put(messageCount);
			buffer.putInt(topicId.intValue());
			buffer.putShort((short) bytes.length);
			buffer.put(bytes);
			addToPacketQueue(prependedBytes, this.sequenceNumber);
			return prependedBytes;
		}

		byte[] packetPayloadBytes = new byte[Utils.MAX_PACKET_PAYLOAD_SIZE];
		ByteBuffer messageBuffer = ByteBuffer.wrap(packetPayloadBytes);
		byte messageCount = 0;
		while (this.messageQueue.size() > 0 && messageCount <= Byte.MAX_VALUE)
		{
			Integer topicId = this.topicIdQueue.remove();
			byte[] messageBytes = this.messageQueue.remove();
			messageBuffer.putInt(topicId.intValue());
			messageBuffer.putShort((short) messageBytes.length);
			messageBuffer.put(messageBytes);
			messageCount++;

			byte[] nextMessageBytes = this.messageQueue.peek();
			if (nextMessageBytes != null)
			{
				if (messageBuffer.remaining() < nextMessageBytes.length + Utils.MESSAGE_HEADER_SIZE)
				{
					break;
				}
			}
		}

		byte[] packetBytes = new byte[Utils.PACKET_HEADER_SIZE + messageBuffer.position()];
		ByteBuffer packetBuffer = ByteBuffer.wrap(packetBytes);
		packetBuffer.put(Utils.PACKET_HEADER_SIZE);
		packetBuffer.put(this.supportsRetransmissions);
		packetBuffer.putLong(++this.sequenceNumber);
		packetBuffer.put(messageCount);
		packetBuffer.put(packetPayloadBytes, 0, messageBuffer.position());
		addToPacketQueue(packetBytes, this.sequenceNumber);
		return packetBytes;
	}

	private void addToPacketQueue(byte[] packetBytes, long sequenceNum)
	{
		if (this.packetCache == null) return;
		this.packetCache.add(packetBytes, sequenceNum);
	}

	// Called by selectorThread
	public Pair<List<byte[]>, Long> getCachedPackets(long firstSequenceNumberRequested, int packetCount)
	{
		if (this.cacheSize == 0) return null;
		long lastSequenceNumberRequested = firstSequenceNumberRequested + packetCount - 1L;
		return this.packetCache.getCachedPackets(firstSequenceNumberRequested, lastSequenceNumberRequested);
	}

	public String getMulticastGroup()
	{
		return this.multicastGroup;
	}

	public void setChannel(DatagramChannel channel)
	{
		this.channel = channel;
	}

	public void sendToChannel() throws IOException
	{
		while (hasOutboundDataRemaining())
		{
			byte[] bytes = getNextPacket();
			// if (this.sequenceNumber % 5L == 0L) return;
			this.channel.setOption(StandardSocketOptions.IP_MULTICAST_IF, this.networkInterface);
			this.channel.send(ByteBuffer.wrap(bytes), this.multicastGroupAddress);
		}
	}

	@Override
	public int getAction()
	{
		return SelectorActionable.SEND_MULTICAST;
	}

	int getMessageQueueSize()
	{
		return this.messageQueue.size();
	}
}