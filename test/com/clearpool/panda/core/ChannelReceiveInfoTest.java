package com.clearpool.panda.core;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ChannelReceiveInfoTest
{
	private InetAddress LOCAL_IP;
	private PandaProperties PROPS;

	@Before
	public void before()
	{
		try
		{
			this.LOCAL_IP = InetAddress.getByName("127.0.0.1");
			this.PROPS = new PandaProperties();
		}
		catch (Exception e)
		{
			this.LOCAL_IP = null;
			this.PROPS= null;
		}
	}

	@Test
	public void testDataReceivedFromSelf() throws IOException
	{
		TestSelectorThread selectorThread = new TestSelectorThread();
		ChannelReceiveInfo channelReceiveInfo = new ChannelReceiveInfo("1.1.1.1", 1, "1.1.1.1:1", this.LOCAL_IP, 10, selectorThread, 10000, false, this.PROPS);

		InetSocketAddress sourceAddress = new InetSocketAddress(this.LOCAL_IP, 10);
		ByteBuffer packetBuffer = createPacketBuffer(PandaUtils.PACKET_HEADER_SIZE, 5, 1);
		channelReceiveInfo.dataReceived(sourceAddress, packetBuffer);
		Assert.assertEquals(0, channelReceiveInfo.getPacketsReceived());
		Assert.assertEquals(0, channelReceiveInfo.getMessagesReceived());
	}

	@Test
	public void testDataReceivedFromOther() throws IOException
	{
		TestSelectorThread selectorThread = new TestSelectorThread();
		ChannelReceiveInfo channelReceiveInfo = new ChannelReceiveInfo("1.1.1.1", 1, "1.1.1.1:1", this.LOCAL_IP, 10, selectorThread, 10000, false, this.PROPS);

		InetSocketAddress sourceAddress = new InetSocketAddress("127.0.0.2", 10);

		ByteBuffer packetBuffer1 = createPacketBuffer(PandaUtils.PACKET_HEADER_SIZE, 5, 1);
		channelReceiveInfo.dataReceived(sourceAddress, packetBuffer1);
		Assert.assertEquals(1, channelReceiveInfo.getPacketsReceived());
		Assert.assertEquals(5, channelReceiveInfo.getMessagesReceived());

		ByteBuffer packetBuffer2 = createPacketBuffer(PandaUtils.PACKET_HEADER_SIZE, 6, 2);
		channelReceiveInfo.dataReceived(sourceAddress, packetBuffer2);
		Assert.assertEquals(2, channelReceiveInfo.getPacketsReceived());
		Assert.assertEquals(11, channelReceiveInfo.getMessagesReceived());
	}

	@Test
	public void testDataReceivedPacketHeaderLengthDifferent() throws IOException
	{
		TestSelectorThread selectorThread = new TestSelectorThread();
		ChannelReceiveInfo channelReceiveInfo = new ChannelReceiveInfo("1.1.1.1", 1, "1.1.1.1:1", this.LOCAL_IP, 10, selectorThread, 10000, false, this.PROPS);

		InetSocketAddress sourceAddress = new InetSocketAddress("127.0.0.2", 10);

		ByteBuffer packetBuffer1 = createPacketBuffer(20, 5, 1);
		channelReceiveInfo.dataReceived(sourceAddress, packetBuffer1);
		Assert.assertEquals(1, channelReceiveInfo.getPacketsReceived());
		Assert.assertEquals(5, channelReceiveInfo.getMessagesReceived());
	}

	private static ByteBuffer createPacketBuffer(int packetHeaderSize, int messageCount, long sequenceNumber)
	{
		String topic = "1";
		ByteBuffer buffer = ByteBuffer.allocate(packetHeaderSize + (PandaUtils.MESSAGE_HEADER_FIXED_SIZE + topic.length() + 4) * messageCount);
		buffer.put((byte) packetHeaderSize);
		buffer.put((byte) 1); // supports retrans
		buffer.putLong(sequenceNumber);
		buffer.put((byte) messageCount);
		int position = buffer.position();
		for (int i = 0; i < packetHeaderSize - position; i++)
		{
			buffer.put((byte) 0); // filler bytes in case header is longer than expected
		}
		for (int i = 0; i < messageCount; i++)
		{
			buffer.put((byte) 1);
			buffer.put(topic.getBytes());
			buffer.putShort((short) 4); // messageLength
			buffer.putInt(0);
		}
		buffer.rewind();
		return buffer;
	}
}