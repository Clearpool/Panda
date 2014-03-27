package com.clearpool.panda.core;

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

	public void subscribe(String topic, String ip, int port, String multicastGroup, String interfaceIp, PandaDataListener listener, int recvBufferSize, boolean skipGaps)
	{
		ChannelReceiveInfo receiveInfo = getChannelReceiverInfo(ip, port, multicastGroup, interfaceIp, recvBufferSize, skipGaps);
		synchronized (receiveInfo)
		{
			receiveInfo.registerTopicListener(topic, listener);
		}
	}

	private ChannelReceiveInfo getChannelReceiverInfo(String multicastIp, int multicastPort, String multicastGroup, String interfaceIp, int recvBufferSize, boolean skipGaps)
	{
		ChannelReceiveInfo receiveInfo = this.channelInfos.get(multicastGroup);
		if (receiveInfo == null)
		{
			synchronized (this)
			{
				receiveInfo = this.channelInfos.get(multicastGroup);
				if (receiveInfo == null)
				{
					receiveInfo = new ChannelReceiveInfo(multicastIp, multicastPort, multicastGroup, interfaceIp, this.bindPort, this.selectorThread, recvBufferSize, skipGaps);
					this.channelInfos.put(multicastGroup, receiveInfo);
				}
			}
		}
		return receiveInfo;
	}
}