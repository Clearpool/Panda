package panda.core.containers;

import java.nio.channels.spi.AbstractSelectableChannel;

import panda.core.IAction;


public class TcpRegistration implements IAction
{
	private final AbstractSelectableChannel channel;
	private final int interestOps;
	private final Object attachment;
	
	public TcpRegistration(AbstractSelectableChannel channel, int interestOps, Object attachment)
	{
		this.channel = channel;
		this.interestOps = interestOps;
		this.attachment = attachment;
	}

	public AbstractSelectableChannel getChannel()
	{
		return this.channel;
	}
	
	public int getInterestOps()
	{
		return this.interestOps;
	}

	public Object getAttachment()
	{
		return this.attachment;
	}

	@Override
	public int getAction()
	{
		return IAction.REGISTER_TCP_ACTION;
	}
}