package panda.core;

import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import panda.core.containers.TopicInfo;
import panda.core.datastructures.Pair;
import panda.utils.Utils;

//TODO - FEATURE -handle large messages
public class Sender
{
	private final static Logger LOGGER = Logger.getLogger(Sender.class.getName());

	private final SelectorThread selectorThread;
	private final int cacheSize;
	private final Map<String, ChannelSendInfo> channelInfos;

	public Sender(SelectorThread selectorThread, ServerSocketChannel channel, int cacheSize)
	{
		this.selectorThread = selectorThread;
		this.cacheSize = cacheSize;
		registierRetransmissionChannel(channel);
		this.channelInfos = new ConcurrentHashMap<String, ChannelSendInfo>();
	}

	private void registierRetransmissionChannel(ServerSocketChannel channel)
	{
		try
		{
			this.selectorThread.registerTcpChannelAction(channel, 16, this);
		}
		catch (Exception e)
		{
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	public void send(TopicInfo topicInfo, String interfaceIp, byte[] bytes) throws Exception
	{
		if (bytes.length > Utils.MAX_MESSAGE_PAYLOAD_SIZE)
			throw new Exception("Message length over size=" + Utils.MAX_MESSAGE_PAYLOAD_SIZE + " not allowed.");
		ChannelSendInfo sendInfo = getChannelSendInfo(topicInfo, interfaceIp);
		synchronized (sendInfo)
		{
			sendInfo.addMessageToSendQueue(topicInfo.getTopicId(), bytes);
			this.selectorThread.sendToMulticastChannel(sendInfo);
		}
	}

	private ChannelSendInfo getChannelSendInfo(TopicInfo topicInfo, String interfaceIp)
	{
		ChannelSendInfo sendInfo = this.channelInfos.get(topicInfo.getMulticastGroup());
		if (sendInfo == null)
		{
			sendInfo = new ChannelSendInfo(topicInfo.getIp(), topicInfo.getPort().intValue(), topicInfo.getMulticastGroup(), this.cacheSize, interfaceIp);

			this.channelInfos.put(topicInfo.getMulticastGroup(), sendInfo);
		}
		return sendInfo;
	}

	public void processGapRequest(SocketChannel channel, ByteBuffer tcpBuffer)
	{
		long startSequenceNumber = tcpBuffer.getLong();
		int packetCount = tcpBuffer.getInt();
		byte[] bytes = new byte[tcpBuffer.remaining()];
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
		this.selectorThread.registerTcpChannelAction(channel, 4, response);
	}

	public void close()
	{
		
	}
}