package com.clearpool.panda.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.codahale.metrics.MetricRegistry;

class Receiver
{
	private final SelectorThread selectorThread;
	private final int bindPort;
	final Map<String, ChannelReceiveInfo> channelInfos;

	Receiver(SelectorThread selectorThread, int bindPort)
	{
		this.selectorThread = selectorThread;
		this.bindPort = bindPort;
		this.channelInfos = new ConcurrentHashMap<String, ChannelReceiveInfo>();
	}

	void subscribe(String topic, String ip, int port, String multicastGroup, String interfaceIp, PandaDataListener listener, int recvBufferSize, boolean skipGaps)
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

	void recordStats(MetricRegistry metricsRegistry, String prefix)
	{
		for (ChannelReceiveInfo channelInfo : this.channelInfos.values())
		{
			long packetsReceived = channelInfo.getPacketsReceived();
			long bytesReceived = channelInfo.getBytesReceived();
			long messagesReceived = channelInfo.getMessagesReceived();
			long messagesHandled = channelInfo.getMessagesHandled();

			metricsRegistry.meter(prefix + "-PACKETS_RECEIVED-" + channelInfo.getMulticastGroup()).mark(packetsReceived);
			metricsRegistry.meter(prefix + "-BYTES_RECEIVED-" + channelInfo.getMulticastGroup()).mark(bytesReceived);
			metricsRegistry.meter(prefix + "-MESSAGES_RECEIVED-" + channelInfo.getMulticastGroup()).mark(messagesReceived);
			metricsRegistry.meter(prefix + "-MESSAGES_HANDLED-" + channelInfo.getMulticastGroup()).mark(messagesHandled);
		}
	}
}