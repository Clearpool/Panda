package panda.core;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

@SuppressWarnings("static-method")
public class ChannelReceiveInfoTest
{
	@Test
	public void testDataReceivedFromSelf() throws IOException
	{
		TestSelectorThread selectorThread = new TestSelectorThread();
		ChannelReceiveInfo channelReceiveInfo = new ChannelReceiveInfo("1.1.1.1", 1, "1.1.1.1:1", "127.0.0.1", 10, selectorThread, 10000);

		InetSocketAddress sourceAddress = new InetSocketAddress("127.0.0.1", 10);
		ByteBuffer packetBuffer = createPacketBuffer(Utils.PACKET_HEADER_SIZE, 5, 1);
		channelReceiveInfo.dataReceived(sourceAddress, packetBuffer);
		Assert.assertEquals(0, channelReceiveInfo.getPacketsProcessed());
		Assert.assertEquals(0, channelReceiveInfo.getMessagesProcessed());
	}

	@Test
	public void testDataReceivedFromOther() throws IOException
	{
		TestSelectorThread selectorThread = new TestSelectorThread();
		ChannelReceiveInfo channelReceiveInfo = new ChannelReceiveInfo("1.1.1.1", 1, "1.1.1.1:1", "127.0.0.1", 10, selectorThread, 10000);

		InetSocketAddress sourceAddress = new InetSocketAddress("127.0.0.2", 10);

		ByteBuffer packetBuffer1 = createPacketBuffer(Utils.PACKET_HEADER_SIZE, 5, 1);
		channelReceiveInfo.dataReceived(sourceAddress, packetBuffer1);
		Assert.assertEquals(1, channelReceiveInfo.getPacketsProcessed());
		Assert.assertEquals(5, channelReceiveInfo.getMessagesProcessed());

		ByteBuffer packetBuffer2 = createPacketBuffer(Utils.PACKET_HEADER_SIZE, 6, 2);
		channelReceiveInfo.dataReceived(sourceAddress, packetBuffer2);
		Assert.assertEquals(2, channelReceiveInfo.getPacketsProcessed());
		Assert.assertEquals(11, channelReceiveInfo.getMessagesProcessed());
	}

	@Test
	public void testDataReceivedPacketHeaderLengthDifferent() throws IOException
	{
		TestSelectorThread selectorThread = new TestSelectorThread();
		ChannelReceiveInfo channelReceiveInfo = new ChannelReceiveInfo("1.1.1.1", 1, "1.1.1.1:1", "127.0.0.1", 10, selectorThread, 10000);

		InetSocketAddress sourceAddress = new InetSocketAddress("127.0.0.2", 10);

		ByteBuffer packetBuffer1 = createPacketBuffer(20, 5, 1);
		channelReceiveInfo.dataReceived(sourceAddress, packetBuffer1);
		Assert.assertEquals(1, channelReceiveInfo.getPacketsProcessed());
		Assert.assertEquals(5, channelReceiveInfo.getMessagesProcessed());
	}

	private static ByteBuffer createPacketBuffer(int packetHeaderSize, int messageCount, long sequenceNumber)
	{
		String topic = "1";
		ByteBuffer buffer = ByteBuffer.allocate(packetHeaderSize + (Utils.MESSAGE_HEADER_FIXED_SIZE + topic.length() + 4) * messageCount);
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