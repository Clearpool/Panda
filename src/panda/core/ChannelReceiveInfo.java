package panda.core;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import panda.core.containers.TopicInfo;


public class ChannelReceiveInfo
{	
	private final String multicastIp;
	private final int multicastPort;
	private final String multicastGroup;
	private final String localIp;
	private final int bindPort;
	private final SelectorThread selectorThread;
	private final Map<Integer, Set<IDataListener>> topicToListeners;
	private final Map<String, ChannelReceiveSequencer> sourceInfos;

	public ChannelReceiveInfo(String multicastIp, int multicastPort, String multicastGroup, String localIp, int bindPort, SelectorThread selectorThread, int recvBufferSize)
	{
		this.multicastIp = multicastIp;
		this.multicastPort = multicastPort;
		this.multicastGroup = multicastGroup;
		this.localIp = localIp;
		this.bindPort = bindPort;
		this.selectorThread = selectorThread;
		this.topicToListeners = new HashMap<Integer, Set<IDataListener>>();
		this.sourceInfos = new HashMap<String, ChannelReceiveSequencer>();
		this.selectorThread.subscribeToMulticastChannel(this.multicastIp, this.multicastPort, this.multicastGroup, this.localIp, this, recvBufferSize);
	}

	//Called by app thread
	public void registerTopicListener(TopicInfo topicInfo, IDataListener listener)
	{
		Set<IDataListener> listeners = this.topicToListeners.get(topicInfo.getTopicId());
		if(listeners == null)
		{
			listeners = new HashSet<IDataListener>();
			this.topicToListeners.put(topicInfo.getTopicId(), listeners);
		}
		listeners.add(listener);
	}
	
	//Called by selectorThread
	public void dataReceived(InetSocketAddress sourceAddress, ByteBuffer packetBuffer)
	{
		//Read packet header
		int packetPosition = packetBuffer.position();
		byte packetHeaderLength = packetBuffer.get();
		
		//Discard any packets originated by this process
		String incomingSourceAddress = sourceAddress.getAddress().getHostAddress();
		int incomingSourcePort = sourceAddress.getPort();
		if(incomingSourcePort == this.bindPort && incomingSourceAddress.equals(this.localIp)) return;
	
		//Continue reading packet header
		boolean supportsRetranmissions = packetBuffer.get() > 0;
		long sequenceNumber = packetBuffer.getLong();
		byte messageCount = packetBuffer.get();
		
		//In case packet header changes in future, adjust the position accordingly
		if(packetPosition + packetHeaderLength != packetBuffer.position())
		{
			packetBuffer.position(packetPosition + packetHeaderLength);
		}
		
		//Check for gaps
		ChannelReceiveSequencer sourceInfo = getSourceInfo(incomingSourcePort, incomingSourceAddress);
		sourceInfo.packetReceived(supportsRetranmissions, incomingSourcePort, sequenceNumber, messageCount, packetBuffer);
	}
	
	private ChannelReceiveSequencer getSourceInfo(int incomingPid, String sourceAddress)
	{
		String sourceInfoKey = incomingPid + "@" + sourceAddress;
		ChannelReceiveSequencer sourceInfo = this.sourceInfos.get(sourceInfoKey);
		if(sourceInfo == null)
		{
			sourceInfo = new ChannelReceiveSequencer(this.selectorThread, sourceInfoKey, this.multicastGroup, sourceAddress, this);
			this.sourceInfos.put(sourceInfoKey, sourceInfo);
		}
		return sourceInfo;
	}

	//Called by selectorThread via FmcChannelReceiveSourceInfo
	public void parseAndDeliverToListeners(byte messageCount, ByteBuffer packetBuffer)
	{
		//Parse messages and deliver to listeners
		for(int i=0; i<messageCount; i++)
		{
			Integer incomingTopicId = Integer.valueOf(packetBuffer.getInt());
			short messageLength = packetBuffer.getShort();
			Set<IDataListener> listeners = this.topicToListeners.get(incomingTopicId);
			if(listeners != null)
			{
				byte[] messageBytes = new byte[messageLength];
				packetBuffer.get(messageBytes);
				ByteBuffer messageBuffer = ByteBuffer.wrap(messageBytes);
				for(IDataListener listener : listeners)
				{
					listener.receivedPandaData(incomingTopicId.intValue(), messageBuffer);
				}
			}
			else
			{
				packetBuffer.position(packetBuffer.position() + messageLength);
			}
		}
	}
}