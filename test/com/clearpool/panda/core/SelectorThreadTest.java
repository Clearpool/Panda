package com.clearpool.panda.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

import org.junit.Test;

@SuppressWarnings("static-method")
public class SelectorThreadTest
{
	private static final String LONG_MESSAGE_TOPIC = "RICKROLL";
	private static final String LONG_MESSAGE = "We're no strangers to love|You know the rules and so do I|A full commitment's what I'm thinking of|You wouldn't get this from any other guy||I just wanna tell you how I'm feeling|Gotta make you understand||Never gonna give you up|Never gonna let you down|Never gonna run around and desert you|Never gonna make you cry|Never gonna say goodbye|Never gonna tell a lie and hurt you||We've known each other for so long|Your heart's been aching, but|You're too shy to say it|Inside, we both know what's been going on|We know the game and we're gonna play it||And if you ask me how I'm feeling|Don't tell me you're too blind to see||Never gonna give you up|Never gonna let you down|Never gonna run around and desert you|Never gonna make you cry|Never gonna say goodbye|Never gonna tell a lie and hurt you||Never gonna give you up|Never gonna let you down|Never gonna run around and desert you|Never gonna make you cry|Never gonna say goodbye|Never gonna tell a lie and hurt you||(Ooh, give you up)|(Ooh, give you up)|Never gonna give, never gonna give|(Give you up)|Never gonna give, never gonna give|(Give you up)||We've known each other for so long|Your heart's been aching, but|You're too shy to say it|Inside, we both know what's been going on|We know the game and we're gonna play it||I just wanna tell you how I'm feeling|Gotta make you understand||Never gonna give you up|Never gonna let you down|Never gonna run around and desert you|Never gonna make you cry|Never gonna say goodbye|Never gonna tell a lie and hurt you||Never gonna give you up|Never gonna let you down|Never gonna run around and desert you|Never gonna make you cry|Never gonna say goodbye|Never gonna tell a lie and hurt you||Never gonna give you up|Never gonna let you down|Never gonna run around and desert you|Never gonna make you cry|Never gonna say goodbye|Never gonna tell a lie and hurt you";

	private static TestChannelSendInfo SENDINFO = sendinfo();

	private static TestChannelSendInfo sendinfo()
	{
		try
		{
			InetAddress localIp = InetAddress.getByName("127.0.0.1");
			return new TestChannelSendInfo("127.0.0.1", 0, "127.0.0.1:0", 0, localIp, null);
		}
		catch (Exception e)
		{
		}
		return null;
	}

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

	@Test
	public void testSendMulticastPacketSingleLongMessage()
	{
		Queue<SelectorActionable> q = new LinkedList<SelectorActionable>();
		String topic = LONG_MESSAGE_TOPIC;
		String message = LONG_MESSAGE;
		q.offer(new ChannelSendDetail(SENDINFO, topic, message.getBytes()));
		SelectorThread.sendMulticastData((ChannelSendDetail) q.poll(), q);
		assertEquals(PandaUtils.PACKET_HEADER_SIZE + PandaUtils.MESSAGE_HEADER_FIXED_SIZE + topic.length() + message.length(), SENDINFO.getMulticastBytes().length);
		// Read Topic From Sent Packet
		String sentTopic = new String(SENDINFO.getMulticastBytes()).substring(PandaUtils.PACKET_HEADER_SIZE + 1, PandaUtils.PACKET_HEADER_SIZE + topic.length() + 1);
		assertTrue(LONG_MESSAGE_TOPIC.equals(sentTopic));
		// Read Message From Sent Packet
		String sentMessage = new String(SENDINFO.getMulticastBytes()).substring(PandaUtils.PACKET_HEADER_SIZE + PandaUtils.MESSAGE_HEADER_FIXED_SIZE + topic.length());
		assertTrue(LONG_MESSAGE.equals(sentMessage));
		assertEquals(0, q.size());
	}

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

	@Test
	public void testSendMulticastPacketMultipleLongMessages()
	{
		Queue<SelectorActionable> q = new LinkedList<SelectorActionable>();
		String topic1 = LONG_MESSAGE_TOPIC + "1";
		String topic2 = "TOPIC2";
		String topic3 = LONG_MESSAGE_TOPIC + "22";
		String message1 = LONG_MESSAGE + "1";
		String message2 = "this is a short message";
		String message3 = LONG_MESSAGE + "22";
		q.offer(new ChannelSendDetail(SENDINFO, topic1, message1.getBytes()));
		q.offer(new ChannelSendDetail(SENDINFO, topic2, message2.getBytes()));
		q.offer(new ChannelSendDetail(SENDINFO, topic3, message3.getBytes()));

		SelectorThread.sendMulticastData((ChannelSendDetail) q.poll(), q);
		assertEquals(PandaUtils.PACKET_HEADER_SIZE + PandaUtils.MESSAGE_HEADER_FIXED_SIZE + topic1.length() + message1.length(), SENDINFO.getMulticastBytes().length);
		assertEquals(2, q.size());
		// Read Topic1 From Sent Packet
		String sentTopic = new String(SENDINFO.getMulticastBytes()).substring(PandaUtils.PACKET_HEADER_SIZE + 1, PandaUtils.PACKET_HEADER_SIZE + topic1.length() + 1);
		assertTrue(topic1.equals(sentTopic));
		// Read Message1 From Sent Packet
		String sentMessage = new String(SENDINFO.getMulticastBytes()).substring(PandaUtils.PACKET_HEADER_SIZE + PandaUtils.MESSAGE_HEADER_FIXED_SIZE + topic1.length());
		assertTrue(message1.equals(sentMessage));

		// Short Message
		SelectorThread.sendMulticastData((ChannelSendDetail) q.poll(), q);
		assertEquals(PandaUtils.PACKET_HEADER_SIZE + PandaUtils.MESSAGE_HEADER_FIXED_SIZE + topic2.length() + message2.length(), SENDINFO.getMulticastBytes().length);
		assertEquals(1, q.size());

		SelectorThread.sendMulticastData((ChannelSendDetail) q.poll(), q);
		assertEquals(PandaUtils.PACKET_HEADER_SIZE + PandaUtils.MESSAGE_HEADER_FIXED_SIZE + topic3.length() + message3.length(), SENDINFO.getMulticastBytes().length);
		assertEquals(0, q.size());
		// Read Topic3 From Sent Packet
		sentTopic = new String(SENDINFO.getMulticastBytes()).substring(PandaUtils.PACKET_HEADER_SIZE + 1, PandaUtils.PACKET_HEADER_SIZE + topic3.length() + 1);
		assertTrue(topic3.equals(sentTopic));
		// Read Message3 From Sent Packet
		sentMessage = new String(SENDINFO.getMulticastBytes()).substring(PandaUtils.PACKET_HEADER_SIZE + PandaUtils.MESSAGE_HEADER_FIXED_SIZE + topic3.length());
		assertTrue(message3.equals(sentMessage));
	}

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

	@Test
	public void testSendMulticastPacketJira3960()
	{
		/**
		 * http://jira.fusionts.corp/browse/CLEAR-3960 - BufferOverflowException in Panda SelectorThread
		 */
		Queue<SelectorActionable> q = new LinkedList<SelectorActionable>();
		String topic = "TOPIC";
		byte[] messageBytes = new byte[PandaUtils.PANDA_PACKET_MESSAGE_PAYLOAD_SIZE];
		Arrays.fill(messageBytes, "a".getBytes()[0]);
		q.offer(new ChannelSendDetail(SENDINFO, topic, messageBytes));
		q.offer(new ChannelSendDetail(SENDINFO, topic, messageBytes));

		SelectorThread.sendMulticastData((ChannelSendDetail) q.poll(), q);
		assertEquals(PandaUtils.PACKET_HEADER_SIZE + PandaUtils.MESSAGE_HEADER_FIXED_SIZE + topic.length() + PandaUtils.PANDA_PACKET_MESSAGE_PAYLOAD_SIZE,
				SENDINFO.getMulticastBytes().length);
		assertEquals(1, q.size());

		SelectorThread.sendMulticastData((ChannelSendDetail) q.poll(), q);
		assertEquals(PandaUtils.PACKET_HEADER_SIZE + PandaUtils.MESSAGE_HEADER_FIXED_SIZE + topic.length() + PandaUtils.PANDA_PACKET_MESSAGE_PAYLOAD_SIZE,
				SENDINFO.getMulticastBytes().length);
		assertEquals(0, q.size());
	}
}
