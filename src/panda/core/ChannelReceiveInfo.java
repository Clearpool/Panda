package panda.core;

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
	private final Map<String, Set<PandaDataListener>> topicToListeners;
	private final Set<PandaDataListener> groupListeners;
	private final Map<String, ChannelReceiveSequencer> sourceInfos;

	private long packetsProcessed;
	private long messageProcessed;
	private long messagesHandled;

	public ChannelReceiveInfo(String multicastIp, int multicastPort, String multicastGroup, String localIp, int bindPort, SelectorThread selectorThread, int recvBufferSize)
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

		this.packetsProcessed = 0;
		this.messageProcessed = 0;
		this.messagesHandled = 0;
	}

	// Called by app thread
	public void registerTopicListener(PandaTopicInfo topicInfo, PandaDataListener listener)
	{
		Set<PandaDataListener> topicListeners = this.topicToListeners.get(topicInfo.getTopic());
		if (topicListeners == null)
		{
			topicListeners = new HashSet<PandaDataListener>();
			this.topicToListeners.put(topicInfo.getTopic(), topicListeners);
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

	private ChannelReceiveSequencer getSourceInfo(int incomingPid, String sourceAddress)
	{
		String sourceInfoKey = incomingPid + "@" + sourceAddress;
		ChannelReceiveSequencer sourceInfo = this.sourceInfos.get(sourceInfoKey);
		if (sourceInfo == null)
		{
			sourceInfo = new ChannelReceiveSequencer(this.selectorThread, sourceInfoKey, this.multicastGroup, sourceAddress, this, MAX_SOURCE_DROP_THRESHOLD);
			this.sourceInfos.put(sourceInfoKey, sourceInfo);
		}
		return sourceInfo;
	}

	// Called by selectorThread via FmcChannelReceiveSourceInfo
	public void parseAndDeliverToListeners(byte messageCount, ByteBuffer packetBuffer)
	{
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
		this.messageProcessed += messageCount;
		this.packetsProcessed++;
	}

	public void deliverErrorToListeners(PandaErrorCode errorCode, String message, Throwable throwable)
	{
		for (PandaDataListener listener : this.groupListeners)
		{
			listener.receivedPandaError(errorCode, this.multicastGroup, message, throwable);
		}
	}

	long getPacketsProcessed()
	{
		return this.packetsProcessed;
	}

	long getMessagesProcessed()
	{
		return this.messageProcessed;
	}

	long getMessagesHandled()
	{
		return this.messagesHandled;
	}
}