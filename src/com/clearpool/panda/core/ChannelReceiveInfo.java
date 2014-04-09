package com.clearpool.panda.core;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class ChannelReceiveInfo
{
	private static final int MAX_SOURCE_DROP_THRESHOLD = 1000000;

	private final String multicastIp;
	private final int multicastPort;
	private final String multicastGroup;
	private final String localIp;
	private final int bindPort;
	private final SelectorThread selectorThread;
	private final boolean skipGaps;
	private final Map<String, Set<PandaDataListener>> topicToListeners;
	private final Set<PandaDataListener> groupListeners;
	private final Map<String, ChannelReceiveSequencer> sourceInfos;

	private long packetsReceived;
	private long bytesReceived;
	private long messagesReceived;
	private long messagesHandled;

	public ChannelReceiveInfo(String multicastIp, int multicastPort, String multicastGroup, String localIp, int bindPort, SelectorThread selectorThread, int recvBufferSize,
			boolean skipGaps)
	{
		this.multicastIp = multicastIp;
		this.multicastPort = multicastPort;
		this.multicastGroup = multicastGroup;
		this.localIp = localIp;
		this.bindPort = bindPort;
		this.selectorThread = selectorThread;
		this.topicToListeners = new HashMap<String, Set<PandaDataListener>>();
		this.groupListeners = new HashSet<PandaDataListener>();
		this.sourceInfos = new HashMap<String, ChannelReceiveSequencer>();
		this.selectorThread.subscribeToMulticastChannel(this.multicastIp, this.multicastPort, this.multicastGroup, this.localIp, this, recvBufferSize);
		this.skipGaps = skipGaps;

		this.packetsReceived = 0;
		this.bytesReceived = 0;
		this.messagesReceived = 0;
		this.messagesHandled = 0;
	}

	// Called by app thread
	public void registerTopicListener(String topic, PandaDataListener listener)
	{
		Set<PandaDataListener> topicListeners = this.topicToListeners.get(topic);
		if (topicListeners == null)
		{
			topicListeners = new HashSet<PandaDataListener>();
			this.topicToListeners.put(topic, topicListeners);
		}
		topicListeners.add(listener);
		this.groupListeners.add(listener);
	}

	// Called by selectorThread
	public void dataReceived(InetSocketAddress sourceAddress, ByteBuffer packetBuffer)
	{
		// Read packet header
		int packetPosition = packetBuffer.position();
		byte packetHeaderLength = packetBuffer.get();

		// Discard any packets originated by this process
		String incomingSourceAddress = sourceAddress.getAddress().getHostAddress();
		int incomingSourcePort = sourceAddress.getPort();
		if (incomingSourcePort == this.bindPort && incomingSourceAddress.equals(this.localIp)) return;

		// Continue reading packet header
		boolean supportsRetranmissions = packetBuffer.get() > 0;
		long sequenceNumber = packetBuffer.getLong();
		byte messageCount = packetBuffer.get();

		// In case packet header changes in future, adjust the position accordingly
		if (packetPosition + packetHeaderLength != packetBuffer.position())
		{
			packetBuffer.position(packetPosition + packetHeaderLength);
		}

		// Check for gaps
		ChannelReceiveSequencer sourceInfo = getSourceInfo(incomingSourcePort, incomingSourceAddress);
		sourceInfo.packetReceived(supportsRetranmissions, incomingSourcePort, sequenceNumber, messageCount, packetBuffer);
	}

	private ChannelReceiveSequencer getSourceInfo(int sourcePort, String sourceAddress)
	{
		String sourceInfoKey = sourcePort + "@" + sourceAddress;
		ChannelReceiveSequencer sourceInfo = this.sourceInfos.get(sourceInfoKey);
		if (sourceInfo == null)
		{
			sourceInfo = new ChannelReceiveSequencer(this.selectorThread, sourceInfoKey, this.multicastGroup, sourceAddress, this, MAX_SOURCE_DROP_THRESHOLD, this.skipGaps);
			this.sourceInfos.put(sourceInfoKey, sourceInfo);
		}
		return sourceInfo;
	}

	// Called by selectorThread via FmcChannelReceiveSourceInfo
	public void parseAndDeliverToListeners(byte messageCount, ByteBuffer packetBuffer)
	{
		this.bytesReceived += packetBuffer.remaining();

		// Parse messages and deliver to listeners
		for (int i = 0; i < messageCount; i++)
		{
			byte incomingTopicLength = packetBuffer.get();
			byte[] incomingTopicBytes = new byte[incomingTopicLength];
			packetBuffer.get(incomingTopicBytes);
			String incomingTopic = new String(incomingTopicBytes);
			short messageLength = packetBuffer.getShort();
			Set<PandaDataListener> listeners = this.topicToListeners.get(incomingTopic);
			if (listeners != null)
			{
				byte[] messageBytes = new byte[messageLength];
				packetBuffer.get(messageBytes);
				ByteBuffer messageBuffer = ByteBuffer.wrap(messageBytes);
				for (PandaDataListener listener : listeners)
				{
					listener.receivedPandaData(incomingTopic, messageBuffer);
				}
				this.messagesHandled++;
			}
			else
			{
				packetBuffer.position(packetBuffer.position() + messageLength);
			}
		}
		this.messagesReceived += messageCount;
		this.packetsReceived++;
	}

	public void deliverErrorToListeners(PandaErrorCode errorCode, String message, Throwable throwable)
	{
		for (PandaDataListener listener : this.groupListeners)
		{
			listener.receivedPandaError(errorCode, this.multicastGroup, message, throwable);
		}
	}

	long getPacketsReceived()
	{
		return this.packetsReceived;
	}

	long getMessagesReceived()
	{
		return this.messagesReceived;
	}

	long getMessagesHandled()
	{
		return this.messagesHandled;
	}
	
	long getBytesReceived()
	{
		return this.bytesReceived;
	}

	String getMulticastGroup()
	{
		return this.multicastGroup;
	}
}