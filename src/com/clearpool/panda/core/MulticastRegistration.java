package com.clearpool.panda.core;

import java.nio.channels.DatagramChannel;



class MulticastRegistration implements SelectorActionable
{
	private final DatagramChannel channel;
	private final String ip;
	private final Object attachment;
	
	public MulticastRegistration(DatagramChannel channel, String ip, Object attachment)
	{
		this.channel = channel;
		this.ip = ip;
		this.attachment = attachment;
	}

	public DatagramChannel getChannel()
	{
		return this.channel;
	}

	public String getIp()
	{
		return this.ip;
	}

	public Object getAttachment()
	{
		return this.attachment;
	}

	@Override
	public int getAction()
	{
		return SelectorActionable.REGISTER_MULTICAST_READ;
	}
}