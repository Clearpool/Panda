package com.clearpool.panda.core;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.channels.spi.AbstractSelectableChannel;

public class TestSelectorThread extends SelectorThread
{

	public TestSelectorThread() throws IOException
	{
		super(new PandaProperties());
	}

	@Override
	public void subscribeToMulticastChannel(String ip, int port, String multicastGroup, InetAddress interfaceIp, ChannelReceiveInfo receiverInfo, int recvBufferSize)
	{

	}

	@Override
	public void registerTcpChannelAction(AbstractSelectableChannel channel, int interestOps, Object attachment)
	{

	}

	@Override
	public boolean shouldMakeConnections()
	{
		return false;
	}
}
