package panda.core;

import org.junit.Assert;
import org.junit.Test;

@SuppressWarnings("static-method")
public class ChannelSendInfoTest
{
	@Test
	public void testGetNextPacketSingleMessage() throws Exception
	{
		ChannelSendInfo channelSendInfo = new ChannelSendInfo("127.0.0.1", 1, "test:1", 10, "127.0.0.1");

		// Add 8 byte message to send queue
		String message = "TEST";
		String topic = "1";
		channelSendInfo.addMessageToSendQueue(topic, message.getBytes());

		byte[] packet = channelSendInfo.getNextPacket();
		Assert.assertEquals(Utils.PACKET_HEADER_SIZE + Utils.MESSAGE_HEADER_FIXED_SIZE + topic.length() + message.length(), packet.length);
		Assert.assertEquals(0, channelSendInfo.getMessageQueueSize());
	}

	@Test
	public void testGetNextPacketMultipleMessages() throws Exception
	{
		ChannelSendInfo channelSendInfo = new ChannelSendInfo("127.0.0.1", 1, "test:1", 10, "127.0.0.1");

		// Add message to send queue
		String message = "TEST";
		String topic = "1";
		channelSendInfo.addMessageToSendQueue(topic, message.getBytes());

		// Add message to send queue
		String message2 = "TEST2";
		String topic2 = "1";
		channelSendInfo.addMessageToSendQueue(topic2, message2.getBytes());

		byte[] packet = channelSendInfo.getNextPacket();
		Assert.assertEquals(Utils.PACKET_HEADER_SIZE + Utils.MESSAGE_HEADER_FIXED_SIZE * 2 + topic.length() + topic2.length() + message.length() + message2.length(), packet.length);
		Assert.assertEquals(0, channelSendInfo.getMessageQueueSize());
	}

	@Test
	public void testGetNextPacketWithMessagesRemaining() throws Exception
	{
		ChannelSendInfo channelSendInfo = new ChannelSendInfo("127.0.0.1", 1, "test:1", 300, "127.0.0.1");

		// Add messages to send queue
		String message = "TEST";
		String topic = "1";
		for (int i = 0; i < 300; i++)
		{
			channelSendInfo.addMessageToSendQueue(topic, message.getBytes());
		}

		byte[] packet1 = channelSendInfo.getNextPacket();
		Assert.assertEquals(Utils.PACKET_HEADER_SIZE + (Utils.MESSAGE_HEADER_FIXED_SIZE + topic.length() + message.length()) * 127, packet1.length);
		Assert.assertEquals(173, channelSendInfo.getMessageQueueSize());

		byte[] packet2 = channelSendInfo.getNextPacket();
		Assert.assertEquals(Utils.PACKET_HEADER_SIZE + (Utils.MESSAGE_HEADER_FIXED_SIZE + topic.length() + message.length()) * 127, packet2.length);
		Assert.assertEquals(46, channelSendInfo.getMessageQueueSize());

		byte[] packet3 = channelSendInfo.getNextPacket();
		Assert.assertEquals(Utils.PACKET_HEADER_SIZE + (Utils.MESSAGE_HEADER_FIXED_SIZE + topic.length() + message.length()) * 46, packet3.length);
		Assert.assertEquals(0, channelSendInfo.getMessageQueueSize());
	}
}