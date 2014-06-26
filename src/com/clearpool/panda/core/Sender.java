package com.clearpool.panda.core;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import com.clearpool.common.datastractures.Pair;
import com.codahale.metrics.MetricRegistry;

class Sender
{
	private final static Logger LOGGER = Logger.getLogger(Sender.class.getName());

	private final SelectorThread selectorThread;
	private final DatagramChannel outDatagramChannel;
	private final int cacheSize;
	private final Map<String, ChannelSendInfo> channelInfos;

	Sender(SelectorThread selectorThread, ServerSocketChannel channel, int cacheSize) throws Exception
	{
		this.selectorThread = selectorThread;
		this.cacheSize = cacheSize;
		this.selectorThread.registerTcpChannelAction(channel, SelectionKey.OP_ACCEPT, this);
		this.outDatagramChannel = createDatagramChannel(channel.socket().getLocalPort());
		this.channelInfos = new ConcurrentHashMap<String, ChannelSendInfo>();
	}

	private static DatagramChannel createDatagramChannel(int port)
	{
		while (true)
		{
			try
			{
				DatagramChannel channel = DatagramChannel.open(StandardProtocolFamily.INET);
				channel.setOption(StandardSocketOptions.SO_REUSEADDR, Boolean.TRUE);
				channel.setOption(StandardSocketOptions.IP_MULTICAST_TTL, Integer.valueOf(255));
				channel.configureBlocking(false);
				channel.bind(new InetSocketAddress(port));
				return channel;
			}
			catch (IOException e)
			{
				LOGGER.info(e.getMessage());
			}
		}
	}

	void send(String topic, String ip, int port, String multicastGroup, String interfaceIp, byte[] bytes) throws Exception
	{
		if (bytes.length > PandaUtils.MAX_UDP_MESSAGE_PAYLOAD_SIZE) throw new Exception("Message length over size=" + PandaUtils.MAX_UDP_MESSAGE_PAYLOAD_SIZE + " not allowed.");
		this.selectorThread.sendToMulticastChannel(getChannelSendInfo(ip, port, multicastGroup, interfaceIp), topic, bytes);
	}

	private ChannelSendInfo getChannelSendInfo(String ip, int port, String multicastGroup, String interfaceIp) throws Exception
	{
		ChannelSendInfo sendInfo = this.channelInfos.get(multicastGroup);
		if (sendInfo == null)
		{
			synchronized (this.channelInfos)
			{
				sendInfo = this.channelInfos.get(multicastGroup);
				if (sendInfo == null)
				{
					sendInfo = new ChannelSendInfo(ip, port, multicastGroup, this.cacheSize, interfaceIp, this.outDatagramChannel);
					this.channelInfos.put(multicastGroup, sendInfo);
				}
			}
		}
		return sendInfo;
	}

	void processGapRequest(SocketChannel channel, ByteBuffer tcpBuffer)
	{
		if (tcpBuffer.remaining() >= PandaUtils.RETRANSMISSION_REQUEST_HEADER_SIZE)
		{
			long startSequenceNumber = tcpBuffer.getLong();
			int packetCount = tcpBuffer.getInt();
			byte multicastGroupLength = tcpBuffer.get();
			if (tcpBuffer.remaining() >= multicastGroupLength)
			{
				byte[] bytes = new byte[multicastGroupLength];
				tcpBuffer.get(bytes);
				String multicastGroup = new String(bytes);

				Pair<List<byte[]>, Long> cachedPackets = null;
				ChannelSendInfo sendInfo = this.channelInfos.get(multicastGroup);
				if (sendInfo != null)
				{
					cachedPackets = sendInfo.getCachedPackets(startSequenceNumber, packetCount);
				}
				else
				{
					LOGGER.severe("Unable to fullfil request because can't find sendinfo for multicastGroup=" + multicastGroup);
				}

				if (cachedPackets == null)
				{
					this.selectorThread.registerTcpChannelAction(channel, SelectionKey.OP_WRITE, new GapResponseManager(channel, null, 0));
				}
				else
				{
					this.selectorThread.registerTcpChannelAction(channel, SelectionKey.OP_WRITE, new GapResponseManager(channel, cachedPackets.getA(), cachedPackets.getB()
							.longValue()));
				}
			}
		}
	}

	void close()
	{

	}

	void recordStats(MetricRegistry metricsRegistry, String prefix)
	{
		for (ChannelSendInfo sendInfo : this.channelInfos.values())
		{
			long packetsSent = sendInfo.getPacketsSent();
			long bytesSent = sendInfo.getBytesSent();
			long packetsResent = sendInfo.getPacketsResent();

			metricsRegistry.meter(prefix + "-PACKETS_SENT-" + sendInfo.getMulticastGroup()).mark(packetsSent);
			metricsRegistry.meter(prefix + "-BYTES_SENT-" + sendInfo.getMulticastGroup()).mark(bytesSent);
			metricsRegistry.meter(prefix + "-PACKETS_RESENT-" + sendInfo.getMulticastGroup()).mark(packetsResent);
		}
	}
}