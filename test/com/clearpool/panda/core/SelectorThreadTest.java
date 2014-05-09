package com.clearpool.panda.core;

import static org.junit.Assert.assertEquals;

import java.util.LinkedList;
import java.util.Queue;

import org.junit.Test;

public class SelectorThreadTest
{
	private static TestChannelSendInfo SENDINFO = sendinfo();

	private static TestChannelSendInfo sendinfo()
	{
		try
		{
			return new TestChannelSendInfo("127.0.0.1", 0, "127.0.0.1:0", 0, "127.0.0.1", null);
		}
		catch (Exception e)
		{
		}
		return null;
	}

	@SuppressWarnings("static-method")
	@Test
	public void testSendMulticastPacketSingleMessage()
	{
		Queue<SelectorActionable> q = new LinkedList<SelectorActionable>();
		String topic = "TOPIC";
		String message = "this is a message";
		q.offer(new ChannelSendDetail(SENDINFO, topic, message.getBytes()));
		SelectorThread.sendMulticastData((ChannelSendDetail) q.poll(), q);
		assertEquals(PandaUtils.PACKET_HEADER_SIZE + PandaUtils.MESSAGE_HEADER_FIXED_SIZE + topic.length() + message.length(), SENDINFO.getMulticastBytes().length);
		assertEquals(0, q.size());
	}

	@SuppressWarnings("static-method")
	@Test
	public void testSendMulticastPacketMultipleMessages()
	{
		Queue<SelectorActionable> q = new LinkedList<SelectorActionable>();
		String topic1 = "TOPIC1";
		String topic2 = "TOPIC2";
		String message1 = "this is a message";
		String message2 = "this is a different message";
		q.offer(new ChannelSendDetail(SENDINFO, topic1, message1.getBytes()));
		q.offer(new ChannelSendDetail(SENDINFO, topic2, message2.getBytes()));
		SelectorThread.sendMulticastData((ChannelSendDetail) q.poll(), q);
		assertEquals(PandaUtils.PACKET_HEADER_SIZE + (2 * PandaUtils.MESSAGE_HEADER_FIXED_SIZE) + topic1.length() + topic2.length() + message1.length() + message2.length(),
				SENDINFO.getMulticastBytes().length);
		assertEquals(0, q.size());
	}

	@SuppressWarnings("static-method")
	@Test
	public void testSendMulticastPacketWithMessagesRemaining()
	{
		Queue<SelectorActionable> q = new LinkedList<SelectorActionable>();
		String topic = "1";
		String message = "TEST";
		for (int n = 0; n < 300; n++)
		{
			q.offer(new ChannelSendDetail(SENDINFO, topic, message.getBytes()));
		}
		SelectorThread.sendMulticastData((ChannelSendDetail) q.poll(), q);
		assertEquals(PandaUtils.PACKET_HEADER_SIZE + (Byte.MAX_VALUE * (PandaUtils.MESSAGE_HEADER_FIXED_SIZE + topic.length() + message.length())),
				SENDINFO.getMulticastBytes().length);
		assertEquals(173, q.size());

		SelectorThread.sendMulticastData((ChannelSendDetail) q.poll(), q);
		assertEquals(PandaUtils.PACKET_HEADER_SIZE + (Byte.MAX_VALUE * (PandaUtils.MESSAGE_HEADER_FIXED_SIZE + topic.length() + message.length())),
				SENDINFO.getMulticastBytes().length);
		assertEquals(46, q.size());

		SelectorThread.sendMulticastData((ChannelSendDetail) q.poll(), q);
		assertEquals(PandaUtils.PACKET_HEADER_SIZE + (46 * (PandaUtils.MESSAGE_HEADER_FIXED_SIZE + topic.length() + message.length())), SENDINFO.getMulticastBytes().length);
		assertEquals(0, q.size());
	}
}
