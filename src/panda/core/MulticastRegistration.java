package panda.core;

import java.nio.channels.DatagramChannel;



class MulticastRegistration implements SelectorActionable
{
	private final DatagramChannel channel;
	private final String ip;
	private final int port;
	private final Object attachment;
	
	public MulticastRegistration(DatagramChannel channel, String ip, int port, Object attachment)
	{
		this.channel = channel;
		this.ip = ip;
		this.port = port;
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

	public int getPort()
	{
		return this.port;
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