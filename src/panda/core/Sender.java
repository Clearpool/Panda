package panda.core;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;


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

	public void publish(PandaTopicInfo topicInfo, String interfaceIp, byte[] bytes) throws Exception
	{
		if (bytes.length > Utils.MAX_MESSAGE_PAYLOAD_SIZE)
			throw new Exception("Message length over size=" + Utils.MAX_MESSAGE_PAYLOAD_SIZE + " not allowed.");
		ChannelSendInfo sendInfo = getChannelSendInfo(topicInfo, interfaceIp);
		synchronized (sendInfo)
		{
			sendInfo.addMessageToSendQueue(topicInfo.getTopic(), bytes);
			this.selectorThread.sendToMulticastChannel(sendInfo);
		}
	}

	private ChannelSendInfo getChannelSendInfo(PandaTopicInfo topicInfo, String interfaceIp) throws Exception
	{
		ChannelSendInfo publishInfo = this.channelInfos.get(topicInfo.getMulticastGroup());
		if (publishInfo == null)
		{
			synchronized (this.channelInfos)
			{
				publishInfo = this.channelInfos.get(topicInfo.getMulticastGroup());
				if(publishInfo == null)
				{
					publishInfo = new ChannelSendInfo(topicInfo.getIp(), topicInfo.getPort().intValue(), topicInfo.getMulticastGroup(), this.cacheSize, interfaceIp);
					this.channelInfos.put(topicInfo.getMulticastGroup(), publishInfo);					
				}
			}
		}
		return publishInfo;
	}

	public void processGapRequest(SocketChannel channel, ByteBuffer tcpBuffer)
	{
		long startSequenceNumber = tcpBuffer.getLong();
		int packetCount = tcpBuffer.getInt();
		byte[] bytes = new byte[tcpBuffer.remaining()];
		tcpBuffer.get(bytes);
		String multicastGroup = new String(bytes);

		Pair<List<byte[]>, Long> cachedPackets = null;
		ChannelSendInfo publishInfo = this.channelInfos.get(multicastGroup);
		if (publishInfo != null)
		{
			synchronized (publishInfo)
			{
				cachedPackets = publishInfo.getCachedPackets(startSequenceNumber, packetCount);
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

	public void close()
	{
		
	}
}