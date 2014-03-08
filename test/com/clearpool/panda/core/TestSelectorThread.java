package com.clearpool.panda.core;

import java.io.IOException;
import java.nio.channels.spi.AbstractSelectableChannel;

import com.clearpool.panda.core.ChannelReceiveInfo;
import com.clearpool.panda.core.SelectorThread;

public class TestSelectorThread extends SelectorThread
{

	public TestSelectorThread() throws IOException
	{
		
	}
	
	@Override
	public void subscribeToMulticastChannel(String ip, int port, String multicastGroup, String interfaceIp, ChannelReceiveInfo receiverInfo, int recvBufferSize)
	{
		
	}
	
	@Override
	public void registerTcpChannelAction(AbstractSelectableChannel channel, int interestOps, Object attachment)
	{
		
	}
}
