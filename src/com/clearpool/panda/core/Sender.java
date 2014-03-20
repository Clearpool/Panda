package com.clearpool.panda.core;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import com.clearpool.common.datastractures.Pair;

class Sender
{
	private final static Logger LOGGER = Logger.getLogger(Sender.class.getName());

	private final SelectorThread selectorThread;
	private final int cacheSize;
	private final Map<String, ChannelSendInfo> channelInfos;

	public Sender(SelectorThread selectorThread, ServerSocketChannel channel, int cacheSize) throws Exception
	{
		this.selectorThread = selectorThread;
		this.cacheSize = cacheSize;
		this.selectorThread.registerTcpChannelAction(channel, SelectionKey.OP_ACCEPT, this);
		this.channelInfos = new ConcurrentHashMap<String, ChannelSendInfo>();
	}

	public void send(String topic, String ip, int port, String multicastGroup, String interfaceIp, byte[] bytes) throws Exception
	{
		if (bytes.length > PandaUtils.MAX_MESSAGE_PAYLOAD_SIZE) throw new Exception("Message length over size=" + PandaUtils.MAX_MESSAGE_PAYLOAD_SIZE + " not allowed.");
		ChannelSendInfo sendInfo = getChannelSendInfo(ip, port, multicastGroup, interfaceIp);
		synchronized (sendInfo)
		{
			sendInfo.addMessageToSendQueue(topic, bytes);
			this.selectorThread.sendToMulticastChannel(sendInfo);
		}
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
					sendInfo = new ChannelSendInfo(ip, port, multicastGroup, this.cacheSize, interfaceIp);
					this.channelInfos.put(multicastGroup, sendInfo);
				}
			}
		}
		return sendInfo;
	}

	public void processGapRequest(SocketChannel channel, ByteBuffer tcpBuffer)
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
					synchronized (sendInfo)
					{
						cachedPackets = sendInfo.getCachedPackets(startSequenceNumber, packetCount);
					}
				}
				else
				{
					LOGGER.severe("Unable to fullfil request because can't find sendinfo for multicastGroup=" + multicastGroup);
				}

				List<byte[]> packets = cachedPackets == null ? null : cachedPackets.getA();
				long firstSequenceNumber = cachedPackets == null ? 0 : cachedPackets.getB().longValue();
				GapResponseManager response = new GapResponseManager(channel, packets, firstSequenceNumber);
				this.selectorThread.registerTcpChannelAction(channel, SelectionKey.OP_WRITE, response);
			}
		}
	}

	public void close()
	{

	}
}