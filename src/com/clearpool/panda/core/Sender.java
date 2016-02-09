package com.clearpool.panda.core;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import com.codahale.metrics.MetricRegistry;

class Sender
{
	private final static Logger LOGGER = Logger.getLogger(Sender.class.getName());

	private final SelectorThread selectorThread;
	private final DatagramChannel outDatagramChannel;
	private final int cacheSize;
	private final Map<String, ChannelSendInfo> channelInfos;
	private final PandaProperties properties;

	Sender(SelectorThread selectorThread, ServerSocketChannel tcpChannel, DatagramChannel udpChannel, int cacheSize, PandaProperties properties) throws Exception
	{
		this.selectorThread = selectorThread;
		this.cacheSize = cacheSize;
		this.selectorThread.registerTcpChannelAction(tcpChannel, SelectionKey.OP_ACCEPT, this);
		this.outDatagramChannel = udpChannel;
		this.channelInfos = new ConcurrentHashMap<String, ChannelSendInfo>();
		this.properties = properties;
	}

	void send(String topic, String ip, int port, String multicastGroup, InetAddress interfaceIp, byte[] bytes) throws Exception
	{
		if (bytes.length > PandaUtils.MAX_PANDA_MESSAGE_SIZE) throw new Exception("Message length over size=" + PandaUtils.MAX_PANDA_MESSAGE_SIZE + " not allowed");
		ChannelSendInfo channelInfo = getChannelSendInfo(ip, port, multicastGroup, interfaceIp);
		this.selectorThread.sendToMulticastChannel(channelInfo, topic, bytes);
		channelInfo.updateTopicStats(topic);
	}

	private ChannelSendInfo getChannelSendInfo(String ip, int port, String multicastGroup, InetAddress interfaceIp) throws Exception
	{
		ChannelSendInfo sendInfo = this.channelInfos.get(multicastGroup);
		if (sendInfo == null)
		{
			synchronized (this.channelInfos)
			{
				sendInfo = this.channelInfos.get(multicastGroup);
				if (sendInfo == null)
				{
					sendInfo = new ChannelSendInfo(ip, port, multicastGroup, this.cacheSize, interfaceIp, this.outDatagramChannel, this.properties);
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
					LOGGER.warning("SERVICING REQUEST - |Group=" + multicastGroup + "|startSequenceNumber=" + startSequenceNumber + "|packetCount=" + packetCount
							+ "|responseStart=0|responsePacketCount=0");
				}
				else
				{
					List<byte[]> respondedPackets = cachedPackets.getA();
					long respondedStartSequenceNumber = cachedPackets.getB().longValue();
					this.selectorThread.registerTcpChannelAction(channel, SelectionKey.OP_WRITE, new GapResponseManager(channel, respondedPackets, respondedStartSequenceNumber));
					LOGGER.warning("SERVICING REQUEST - |Group=" + multicastGroup + "|startSequenceNumber=" + startSequenceNumber + "|packetCount=" + packetCount
							+ "|responseStart=" + respondedStartSequenceNumber + "|responsePacketCount=" + respondedPackets.size());
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

			PandaUtils.updateMeterAsCounter(metricsRegistry.meter(prefix + "-PACKETS_SENT-" + sendInfo.getMulticastGroup()), packetsSent);
			PandaUtils.updateMeterAsCounter(metricsRegistry.meter(prefix + "-BYTES_SENT-" + sendInfo.getMulticastGroup()), bytesSent);
			PandaUtils.updateMeterAsCounter(metricsRegistry.meter(prefix + "-PACKETS_RESENT-" + sendInfo.getMulticastGroup()), packetsResent);

			for (Object topicSent : sendInfo.getTopicSentCounters().keys())
			{
				int topicSentCount = sendInfo.getTopicSentCounters().get(topicSent);
				PandaUtils.updateMeterAsCounter(metricsRegistry.meter(prefix + "-TOPIC_SENT-" + topicSent + "-" + sendInfo.getMulticastGroup()), topicSentCount);
			}
		}
	}
}