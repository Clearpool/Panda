package com.clearpool.panda.core;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.ArrayDeque;
import java.util.List;

import com.clearpool.common.datastractures.Pair;

class ChannelSendInfo implements SelectorActionable
{
	private final InetAddress multicastIp;
	private final int multicastPort;
	private final String multicastGroup;
	private final InetSocketAddress multicastGroupAddress;
	private final NetworkInterface networkInterface;
	private final ArrayDeque<String> topicQueue;
	private final ArrayDeque<byte[]> messageQueue;
	private final int cacheSize;
	private final PacketCache packetCache;
	private final byte supportsRetransmissions;
	private final DatagramChannel channel;

	private long sequenceNumber;
	private long packetsSent;
	private long bytesSent;
	private long packetsResent;

	public ChannelSendInfo(String ip, int port, String multicastGroup, int cacheSize, String interfaceIp, DatagramChannel datagramChannel) throws Exception
	{
		this.multicastIp = InetAddress.getByName(ip);
		this.multicastPort = port;
		this.multicastGroup = multicastGroup;
		this.multicastGroupAddress = new InetSocketAddress(this.multicastIp, this.multicastPort);
		this.networkInterface = NetworkInterface.getByInetAddress(InetAddress.getByName(interfaceIp));
		this.channel = datagramChannel;
		if (this.channel != null) this.channel.setOption(StandardSocketOptions.IP_MULTICAST_IF, this.networkInterface);
		this.topicQueue = new ArrayDeque<String>();
		this.messageQueue = new ArrayDeque<byte[]>();
		this.cacheSize = cacheSize;
		this.packetCache = (this.cacheSize > 0 ? new PacketCache(this.cacheSize) : null);
		this.supportsRetransmissions = ((byte) (this.cacheSize > 0 ? 1 : 0));

		this.packetsSent = 0;
		this.bytesSent = 0;
		this.packetsResent = 0;
	}

	// Called by app thread
	public boolean addMessageToSendQueue(String topic, byte[] bytes)
	{
		if (bytes.length <= PandaUtils.MAX_MESSAGE_PAYLOAD_SIZE)
		{
			this.topicQueue.add(topic);
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
			String topic = this.topicQueue.remove();
			byte[] bytes = this.messageQueue.remove();

			byte messageCount = 1;
			byte[] prependedBytes = new byte[PandaUtils.PACKET_HEADER_SIZE + PandaUtils.MESSAGE_HEADER_FIXED_SIZE + topic.length() + bytes.length];
			ByteBuffer buffer = ByteBuffer.wrap(prependedBytes);
			buffer.put(PandaUtils.PACKET_HEADER_SIZE);
			buffer.put(this.supportsRetransmissions);
			buffer.putLong(++this.sequenceNumber);
			buffer.put(messageCount);
			buffer.put((byte) topic.length());
			buffer.put(topic.getBytes());
			buffer.putShort((short) bytes.length);
			buffer.put(bytes);
			addToPacketQueue(prependedBytes, this.sequenceNumber);
			return prependedBytes;
		}

		byte[] packetPayloadBytes = new byte[PandaUtils.MAX_PACKET_PAYLOAD_SIZE];
		ByteBuffer messageBuffer = ByteBuffer.wrap(packetPayloadBytes);
		byte messageCount = 0;
		while (this.messageQueue.size() > 0 && messageCount < Byte.MAX_VALUE)
		{
			String topic = this.topicQueue.remove();
			byte[] messageBytes = this.messageQueue.remove();
			messageBuffer.put((byte) topic.length());
			messageBuffer.put(topic.getBytes());
			messageBuffer.putShort((short) messageBytes.length);
			messageBuffer.put(messageBytes);
			messageCount++;

			String nextTopic = this.topicQueue.peek();
			byte[] nextMessageBytes = this.messageQueue.peek();
			if (nextMessageBytes != null)
			{
				if (messageBuffer.remaining() < PandaUtils.MESSAGE_HEADER_FIXED_SIZE + nextTopic.length() + nextMessageBytes.length)
				{
					break;
				}
			}
		}

		byte[] packetBytes = new byte[PandaUtils.PACKET_HEADER_SIZE + messageBuffer.position()];
		ByteBuffer packetBuffer = ByteBuffer.wrap(packetBytes);
		packetBuffer.put(PandaUtils.PACKET_HEADER_SIZE);
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
		Pair<List<byte[]>, Long> pair = this.packetCache.getCachedPackets(firstSequenceNumberRequested, lastSequenceNumberRequested);
		if (pair != null)
		{
			this.packetsResent += pair.getA().size();
		}
		return pair;
	}

	public String getMulticastGroup()
	{
		return this.multicastGroup;
	}

	public void sendToChannel() throws IOException
	{
		while (hasOutboundDataRemaining())
		{
			byte[] bytes = getNextPacket();
			// if (this.sequenceNumber % 5L == 0L) return;
			this.channel.send(ByteBuffer.wrap(bytes), this.multicastGroupAddress);
			this.packetsSent++;
			this.bytesSent += bytes.length;
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

	public long getPacketsSent()
	{
		return this.packetsSent;
	}

	public long getBytesSent()
	{
		return this.bytesSent;
	}

	public long getPacketsResent()
	{
		return this.packetsResent;
	}
}