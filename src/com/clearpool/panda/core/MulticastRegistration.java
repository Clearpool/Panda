package com.clearpool.panda.core;

import java.nio.channels.DatagramChannel;



class MulticastRegistration implements SelectorActionable
{
	private final DatagramChannel channel;
	private final String ip;
	private final Object attachment;
	
	MulticastRegistration(DatagramChannel channel, String ip, Object attachment)
	{
		this.channel = channel;
		this.ip = ip;
		this.attachment = attachment;
	}

	DatagramChannel getChannel()
	{
		return this.channel;
	}

	String getIp()
	{
		return this.ip;
	}

	Object getAttachment()
	{
		return this.attachment;
	}

	@Override
	public int getAction()
	{
		return SelectorActionable.REGISTER_MULTICAST_READ;
	}
}