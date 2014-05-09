package com.clearpool.panda.core;

public class ChannelSendDetail implements SelectorActionable
{
	private final ChannelSendInfo channelSendInfo;
	private final String messageTopic;
	private final byte[] messageBytes;

	public ChannelSendDetail(ChannelSendInfo channelSendInfo, String messageTopic, byte[] messageBytes)
	{
		this.channelSendInfo = channelSendInfo;
		this.messageTopic = messageTopic;
		this.messageBytes = messageBytes;
	}

	public ChannelSendInfo getChannelSendInfo()
	{
		return this.channelSendInfo;
	}

	public String getMessageTopic()
	{
		return this.messageTopic;
	}

	public byte[] getMessageBytes()
	{
		return this.messageBytes;
	}

	@Override
	public int getAction()
	{
		return SelectorActionable.SEND_MULTICAST;
	}
}
