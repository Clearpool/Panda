package com.clearpool.panda.core;

import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;

import java.net.InetAddress;
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
	private final InetAddress localIp;
	private final int bindPort;
	private final SelectorThread selectorThread;
	private final boolean skipGaps;
	private final Map<String, PandaDataListener> topicToListener;
	private final Set<PandaDataListener> groupListeners;
	private final Map<InetSocketAddress, ChannelReceiveSequencer> sourceInfos;
	private final TObjectIntMap<String> topicReceivedCounter;
	private final TObjectIntMap<String> topicHandledCounter;
	private final char[] tempTopicArray;

	private long packetsReceived;
	private long bytesReceived;
	private long messagesReceived;
	private long messagesHandled;

	ChannelReceiveInfo(String multicastIp, int multicastPort, String multicastGroup, InetAddress localIp, int bindPort, SelectorThread selectorThread, int recvBufferSize,
			boolean skipGaps, PandaProperties properties)
	{
		this.multicastIp = multicastIp;
		this.multicastPort = multicastPort;
		this.multicastGroup = multicastGroup;
		this.localIp = localIp;
		this.bindPort = bindPort;
		this.selectorThread = selectorThread;
		this.topicToListener = new HashMap<String, PandaDataListener>();
		this.groupListeners = new HashSet<PandaDataListener>();
		this.sourceInfos = new HashMap<InetSocketAddress, ChannelReceiveSequencer>();
		this.skipGaps = skipGaps;
		this.topicReceivedCounter = properties.getBooleanProperty(PandaProperties.MAINTAIN_DETAILED_STATS, false) ? new TObjectIntHashMap<String>() : null;
		this.topicHandledCounter = properties.getBooleanProperty(PandaProperties.MAINTAIN_DETAILED_STATS, false) ? new TObjectIntHashMap<String>() : null;
		this.tempTopicArray = new char[255];

		this.selectorThread.subscribeToMulticastChannel(this.multicastIp, this.multicastPort, this.multicastGroup, this.localIp, this, recvBufferSize);
	}

	// Called by app thread
	PandaDataListener registerTopicListener(String topic, PandaDataListener listener)
	{
		return this.topicToListener.put(topic, listener);
	}

	// Called by selectorThread
	void dataReceived(InetSocketAddress sourceAddress, ByteBuffer packetBuffer)
	{
		// Read packet header
		int packetPosition = packetBuffer.position();
		byte packetHeaderLength = packetBuffer.get();

		// Discard any packets originated by this process
		InetAddress incomingSourceAddress = sourceAddress.getAddress();
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
		getSourceInfo(sourceAddress).packetReceived(supportsRetranmissions, sequenceNumber, messageCount, packetBuffer);
	}

	private ChannelReceiveSequencer getSourceInfo(InetSocketAddress sourceAddress)
	{
		ChannelReceiveSequencer sourceInfo = this.sourceInfos.get(sourceAddress);
		if (sourceInfo == null)
		{
			sourceInfo = new ChannelReceiveSequencer(this.selectorThread, this.multicastGroup, sourceAddress, this, MAX_SOURCE_DROP_THRESHOLD, this.skipGaps);
			this.sourceInfos.put(sourceAddress, sourceInfo);
		}
		return sourceInfo;
	}

	// Called by selectorThread via FmcChannelReceiveSourceInfo
	void parseAndDeliverToListeners(byte messageCount, ByteBuffer packetBuffer)
	{
		this.bytesReceived += packetBuffer.remaining();

		// Parse messages and deliver to listeners
		for (int i = 0; i < messageCount; i++)
		{
			// Parse Topic
			byte incomingTopicLength = packetBuffer.get();
			for (int t = 0; t < incomingTopicLength; t++)
			{
				this.tempTopicArray[t] = (char) packetBuffer.get();
			}
			String incomingTopic = new String(this.tempTopicArray, 0, incomingTopicLength);

			// Parse Message + Deliver a new copy to each listener
			short messageLength = packetBuffer.getShort();
			PandaDataListener listeners = this.topicToListener.get(incomingTopic);
			if (listeners != null)
			{
				byte[] messageBytes = new byte[messageLength];
				packetBuffer.get(messageBytes);
				// for (PandaDataListener listener : listeners)
				{
					listeners.receivedPandaData(incomingTopic, messageBytes);
				}
				this.messagesHandled++;
				if (this.topicHandledCounter != null) this.topicHandledCounter.adjustOrPutValue(incomingTopic, 1, 1);
			}
			else
			{
				packetBuffer.position(packetBuffer.position() + messageLength);
			}
			if (this.topicReceivedCounter != null) this.topicReceivedCounter.adjustOrPutValue(incomingTopic, 1, 1);
		}
		this.messagesReceived += messageCount;
		this.packetsReceived++;
	}

	void deliverErrorToListeners(PandaErrorCode errorCode, String message, Throwable throwable)
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

	Map<InetSocketAddress, ChannelReceiveSequencer> getSourceInfos()
	{
		return this.sourceInfos;
	}

	TObjectIntMap<String> getTopicReceivedCounters()
	{
		return this.topicReceivedCounter;
	}

	TObjectIntMap<String> getTopicHandledCounters()
	{
		return this.topicHandledCounter;
	}

}