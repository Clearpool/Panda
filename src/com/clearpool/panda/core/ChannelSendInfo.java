package com.clearpool.panda.core;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.List;

class ChannelSendInfo
{
	private final InetAddress multicastIp;
	private final int multicastPort;
	private final String multicastGroup;
	private final InetSocketAddress multicastGroupAddress;
	private final NetworkInterface networkInterface;
	private final int cacheSize;
	private final PacketCache packetCache;
	private final byte supportsRetransmissions;
	private final DatagramChannel channel;

	private long sequenceNumber;
	private long packetsSent;
	private long bytesSent;
	private long packetsResent;

	ChannelSendInfo(String ip, int port, String multicastGroup, int cacheSize, String interfaceIp, DatagramChannel datagramChannel) throws Exception
	{
		this.multicastIp = InetAddress.getByName(ip);
		this.multicastPort = port;
		this.multicastGroup = multicastGroup;
		this.multicastGroupAddress = new InetSocketAddress(this.multicastIp, this.multicastPort);
		this.networkInterface = NetworkInterface.getByInetAddress(InetAddress.getByName(interfaceIp));
		this.channel = datagramChannel;
		if (this.channel != null) this.channel.setOption(StandardSocketOptions.IP_MULTICAST_IF, this.networkInterface);
		this.cacheSize = cacheSize;
		this.packetCache = (this.cacheSize > 0 ? new PacketCache(this.cacheSize) : null);
		this.supportsRetransmissions = ((byte) (this.cacheSize > 0 ? 1 : 0));

		this.packetsSent = 0;
		this.bytesSent = 0;
		this.packetsResent = 0;
	}

	private void addToPacketCache(byte[] packetBytes)
	{
		if (this.packetCache == null) return;
		this.packetCache.add(packetBytes, this.sequenceNumber);
	}

	// Called by selectorThread
	Pair<List<byte[]>, Long> getCachedPackets(long firstSequenceNumberRequested, int packetCount)
	{
		if (this.cacheSize == 0) return null;
		Pair<List<byte[]>, Long> pair = this.packetCache.getCachedPackets(firstSequenceNumberRequested, firstSequenceNumberRequested + packetCount - 1);
		if (pair != null)
		{
			this.packetsResent += pair.getA().size();
		}
		return pair;
	}

	String getMulticastGroup()
	{
		return this.multicastGroup;
	}

	void sendToChannel(ByteBuffer buffer) throws IOException
	{
		addToPacketCache(buffer.array());
		this.channel.send(buffer, this.multicastGroupAddress);
		this.packetsSent++;
		this.bytesSent += buffer.capacity();
	}

	long getPacketsSent()
	{
		return this.packetsSent;
	}

	long getBytesSent()
	{
		return this.bytesSent;
	}

	long getPacketsResent()
	{
		return this.packetsResent;
	}

	byte supportsRetransmissions()
	{
		return this.supportsRetransmissions;
	}

	long incrementAndGetSequenceNumber()
	{
		return ++this.sequenceNumber;
	}
}