package com.clearpool.panda.core;

public class ChannelSendDetail implements SelectorActionable
{
	private final ChannelSendInfo channelSendInfo;
	private final String messageTopic;
	private final byte[] messageBytes;

	ChannelSendDetail(ChannelSendInfo channelSendInfo, String messageTopic, byte[] messageBytes)
	{
		this.channelSendInfo = channelSendInfo;
		this.messageTopic = messageTopic;
		this.messageBytes = messageBytes;
	}

	ChannelSendInfo getChannelSendInfo()
	{
		return this.channelSendInfo;
	}

	String getMessageTopic()
	{
		return this.messageTopic;
	}

	byte[] getMessageBytes()
	{
		return this.messageBytes;
	}

	@Override
	public int getAction()
	{
		return SelectorActionable.SEND_MULTICAST;
	}
}
