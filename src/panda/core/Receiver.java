package panda.core;

import java.util.HashMap;
import java.util.Map;



class Receiver
{
	private final SelectorThread selectorThread;
	private final int bindPort;
	private final Map<String, ChannelReceiveInfo> channelInfos;

	public Receiver(SelectorThread selectorThread, int bindPort)
	{
		this.selectorThread = selectorThread;
		this.bindPort = bindPort;
		this.channelInfos = new HashMap<String, ChannelReceiveInfo>();
	}

	public void subscribe(PandaTopicInfo topicInfo, String interfaceIp, PandaDataListener listener, int recvBufferSize)
	{
		ChannelReceiveInfo receiveInfo = getChannelReceiverInfo(topicInfo.getIp(), topicInfo.getPort().intValue(), topicInfo.getMulticastGroup(), interfaceIp, recvBufferSize);
		synchronized (receiveInfo)
		{
			receiveInfo.registerTopicListener(topicInfo, listener);
		}
	}

	private ChannelReceiveInfo getChannelReceiverInfo(String multicastIp, int multicastPort, String multicastGroup, String interfaceIp, int recvBufferSize)
	{
		ChannelReceiveInfo receiveInfo = this.channelInfos.get(multicastGroup);
		if (receiveInfo == null)
		{
			synchronized (this)
			{
				receiveInfo = this.channelInfos.get(multicastGroup);
				if (receiveInfo == null)
				{
					receiveInfo = new ChannelReceiveInfo(multicastIp, multicastPort, multicastGroup, interfaceIp, this.bindPort, this.selectorThread, recvBufferSize);
					this.channelInfos.put(multicastGroup, receiveInfo);
				}
			}
		}
		return receiveInfo;
	}
}