package com.clearpool.panda.core;

import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.codahale.metrics.MetricRegistry;

class Receiver
{
	private final SelectorThread selectorThread;
	private final int bindPort;
	private final Map<String, ChannelReceiveInfo> channelInfos;
	private final PandaProperties properties;

	Receiver(SelectorThread selectorThread, int bindPort, PandaProperties properties)
	{
		this.selectorThread = selectorThread;
		this.bindPort = bindPort;
		this.channelInfos = new ConcurrentHashMap<String, ChannelReceiveInfo>();
		this.properties = properties;
	}

	PandaDataListener subscribe(String topic, String ip, int port, String multicastGroup, InetAddress interfaceIp, PandaDataListener listener, int recvBufferSize, boolean skipGaps)
	{
		ChannelReceiveInfo receiveInfo = getChannelReceiverInfo(ip, port, multicastGroup, interfaceIp, recvBufferSize, skipGaps);
		synchronized (receiveInfo)
		{
			return receiveInfo.registerTopicListener(topic, listener);
		}
	}

	private ChannelReceiveInfo getChannelReceiverInfo(String multicastIp, int multicastPort, String multicastGroup, InetAddress interfaceIp, int recvBufferSize, boolean skipGaps)
	{
		ChannelReceiveInfo receiveInfo = this.channelInfos.get(multicastGroup);
		if (receiveInfo == null)
		{
			synchronized (this)
			{
				receiveInfo = this.channelInfos.get(multicastGroup);
				if (receiveInfo == null)
				{
					receiveInfo = new ChannelReceiveInfo(multicastIp, multicastPort, multicastGroup, interfaceIp, this.bindPort, this.selectorThread, recvBufferSize, skipGaps,
							this.properties);
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

			PandaUtils.updateMeterAsCounter(metricsRegistry.meter(prefix + "-PACKETS_RECEIVED-" + channelInfo.getMulticastGroup()), packetsReceived);
			PandaUtils.updateMeterAsCounter(metricsRegistry.meter(prefix + "-BYTES_RECEIVED-" + channelInfo.getMulticastGroup()), bytesReceived);
			PandaUtils.updateMeterAsCounter(metricsRegistry.meter(prefix + "-MESSAGES_RECEIVED-" + channelInfo.getMulticastGroup()), messagesReceived);
			PandaUtils.updateMeterAsCounter(metricsRegistry.meter(prefix + "-MESSAGES_HANDLED-" + channelInfo.getMulticastGroup()), messagesHandled);

			for (Object topicReceived : channelInfo.getTopicReceivedCounters().keys())
			{
				int topicReceivedCount = channelInfo.getTopicReceivedCounters().get(topicReceived);
				PandaUtils.updateMeterAsCounter(metricsRegistry.meter(prefix + "-TOPIC_RECEIVED-" + topicReceived + "-" + channelInfo.getMulticastGroup()), topicReceivedCount);
			}

			for (Object topicHandled : channelInfo.getTopicHandledCounters().keys())
			{
				int topicHandledCount = channelInfo.getTopicHandledCounters().get(topicHandled);
				PandaUtils.updateMeterAsCounter(metricsRegistry.meter(prefix + "-TOPIC_HANDLED-" + topicHandled + "-" + channelInfo.getMulticastGroup()), topicHandledCount);
			}
		}
	}

	Map<String, ChannelReceiveInfo> getChannelReceiveInfos()
	{
		return this.channelInfos;
	}
}