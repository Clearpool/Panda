package com.clearpool.panda.core;

import java.nio.channels.spi.AbstractSelectableChannel;

class TcpRegistration implements SelectorActionable
{
	private final AbstractSelectableChannel channel;
	private final int interestOps;
	private final Object attachment;

	TcpRegistration(AbstractSelectableChannel channel, int interestOps, Object attachment)
	{
		this.channel = channel;
		this.interestOps = interestOps;
		this.attachment = attachment;
	}

	AbstractSelectableChannel getChannel()
	{
		return this.channel;
	}

	int getInterestOps()
	{
		return this.interestOps;
	}

	Object getAttachment()
	{
		return this.attachment;
	}

	@Override
	public int getAction()
	{
		return SelectorActionable.REGISTER_TCP_ACTION;
	}
}