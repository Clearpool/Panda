package com.clearpool.panda.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;

public class TestChannelSendInfo extends ChannelSendInfo
{
	private byte[] multicastBytes;
	
	public TestChannelSendInfo(String ip, int port, String multicastGroup, int cacheSize, String interfaceIp, DatagramChannel datagramChannel) throws Exception
	{
		super(ip, port, multicastGroup, cacheSize, interfaceIp, datagramChannel);
	}

	@Override
	public void sendToChannel(ByteBuffer buffer) throws IOException
	{
		this.multicastBytes = buffer.array();
	}
	
	public byte[] getMulticastBytes()
	{
		return this.multicastBytes;
	}
}
