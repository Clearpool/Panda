package com.clearpool.panda.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;

import org.junit.Assert;
import org.junit.Test;

import com.clearpool.panda.core.ChannelReceiveInfo;
import com.clearpool.panda.core.ChannelReceiveSequencer;
import com.clearpool.panda.core.GapRequestManager;
import com.clearpool.panda.core.PandaUtils;

@SuppressWarnings("static-method")
public class GapRequestManagerTest
{
	@Test
	public void testProcessGapResponseNoFragment() throws IOException
	{
		TestSelectorThread selectorThread = new TestSelectorThread();
		ChannelReceiveInfo channelReceiveInfo = new ChannelReceiveInfo("1.1.1.1", 1, "1.1.1.1:1", "127.0.0.1", 10, selectorThread, 10000);
		ChannelReceiveSequencer sequencer = new ChannelReceiveSequencer(selectorThread, "key", "1.1.1.1:1", "127.0.0.2", channelReceiveInfo, 1000);
		GapRequestManager gapRequestManager = new GapRequestManager(selectorThread, "1.1.1.1:1", "127.0.0.1", sequencer);
		
		gapRequestManager.sendGapRequest(100, 3, 5);
		ByteBuffer response = createResponse(3, 5);
		gapRequestManager.processGapResponse(null, null, response);
		Assert.assertTrue(gapRequestManager.isResponseHeaderReceived());
		Assert.assertEquals(3, gapRequestManager.getResponseFirstSequenceNumber());
		Assert.assertEquals(5, gapRequestManager.getResponsePacketCount());
		Assert.assertEquals(0, gapRequestManager.getPacketsRemainingToDeliver());
	}

	@Test
	public void testProcessGapResponseResponseHeaderFragment() throws IOException
	{
		TestSelectorThread selectorThread = new TestSelectorThread();
		ChannelReceiveInfo channelReceiveInfo = new ChannelReceiveInfo("1.1.1.1", 1, "1.1.1.1:1", "127.0.0.1", 10, selectorThread, 10000);
		ChannelReceiveSequencer sequencer = new ChannelReceiveSequencer(selectorThread, "key", "1.1.1.1:1", "127.0.0.2", channelReceiveInfo, 1000);
		GapRequestManager gapRequestManager = new GapRequestManager(selectorThread, "1.1.1.1:1", "127.0.0.1", sequencer);
		
		gapRequestManager.sendGapRequest(100, 3, 5);
		ByteBuffer response = createResponse(3, 5);
		
		ByteBuffer response1 = ByteBuffer.allocate(2);
		while(response1.hasRemaining()) response1.put(response.get());
		response1.rewind();
		
		gapRequestManager.processGapResponse(null, null, response1);
		Assert.assertFalse(gapRequestManager.isResponseHeaderReceived());
		Assert.assertEquals(0, gapRequestManager.getResponseFirstSequenceNumber());
		Assert.assertEquals(0, gapRequestManager.getResponsePacketCount());
		Assert.assertEquals(0, gapRequestManager.getPacketsRemainingToDeliver());
		
		ByteBuffer response2 = ByteBuffer.allocate(response.remaining());
		while(response2.hasRemaining()) response2.put(response.get());
		response2.rewind();
		
		gapRequestManager.processGapResponse(null, null, response2);
		Assert.assertTrue(gapRequestManager.isResponseHeaderReceived());
		Assert.assertEquals(3, gapRequestManager.getResponseFirstSequenceNumber());
		Assert.assertEquals(5, gapRequestManager.getResponsePacketCount());
		Assert.assertEquals(0, gapRequestManager.getPacketsRemainingToDeliver());
	}

	@Test
	public void testProcessGapResponsePacketHeaderFragment() throws IOException
	{
		TestSelectorThread selectorThread = new TestSelectorThread();
		ChannelReceiveInfo channelReceiveInfo = new ChannelReceiveInfo("1.1.1.1", 1, "1.1.1.1:1", "127.0.0.1", 10, selectorThread, 10000);
		ChannelReceiveSequencer sequencer = new ChannelReceiveSequencer(selectorThread, "key", "1.1.1.1:1", "127.0.0.2", channelReceiveInfo, 1000);
		GapRequestManager gapRequestManager = new GapRequestManager(selectorThread, "1.1.1.1:1", "127.0.0.1", sequencer);
		
		gapRequestManager.sendGapRequest(100, 3, 5);
		ByteBuffer response = createResponse(3, 5);
		
		ByteBuffer response1 = ByteBuffer.allocate(14);
		while(response1.hasRemaining()) response1.put(response.get());
		response1.rewind();
		
		gapRequestManager.processGapResponse(null, null, response1);
		Assert.assertTrue(gapRequestManager.isResponseHeaderReceived());
		Assert.assertEquals(3, gapRequestManager.getResponseFirstSequenceNumber());
		Assert.assertEquals(5, gapRequestManager.getResponsePacketCount());
		Assert.assertEquals(5, gapRequestManager.getPacketsRemainingToDeliver());
		
		ByteBuffer response2 = ByteBuffer.allocate(response.remaining());
		while(response2.hasRemaining()) response2.put(response.get());
		response2.rewind();
		
		gapRequestManager.processGapResponse(null, null, response2);
		Assert.assertTrue(gapRequestManager.isResponseHeaderReceived());
		Assert.assertEquals(3, gapRequestManager.getResponseFirstSequenceNumber());
		Assert.assertEquals(5, gapRequestManager.getResponsePacketCount());
		Assert.assertEquals(0, gapRequestManager.getPacketsRemainingToDeliver());
	}

	@Test
	public void testProcessGapResponsePacketFragment() throws IOException
	{
		TestSelectorThread selectorThread = new TestSelectorThread();
		ChannelReceiveInfo channelReceiveInfo = new ChannelReceiveInfo("1.1.1.1", 1, "1.1.1.1:1", "127.0.0.1", 10, selectorThread, 10000);
		ChannelReceiveSequencer sequencer = new ChannelReceiveSequencer(selectorThread, "key", "1.1.1.1:1", "127.0.0.2", channelReceiveInfo, 1000);
		GapRequestManager gapRequestManager = new GapRequestManager(selectorThread, "1.1.1.1:1", "127.0.0.1", sequencer);
		
		gapRequestManager.sendGapRequest(100, 3, 5);
		ByteBuffer response = createResponse(3, 5);
		
		ByteBuffer response1 = ByteBuffer.allocate(83);
		while(response1.hasRemaining()) response1.put(response.get());
		response1.rewind();
		
		gapRequestManager.processGapResponse(null, null, response1);
		Assert.assertTrue(gapRequestManager.isResponseHeaderReceived());
		Assert.assertEquals(3, gapRequestManager.getResponseFirstSequenceNumber());
		Assert.assertEquals(5, gapRequestManager.getResponsePacketCount());
		Assert.assertEquals(4, gapRequestManager.getPacketsRemainingToDeliver());
		
		ByteBuffer response2 = ByteBuffer.allocate(response.remaining());
		while(response2.hasRemaining()) response2.put(response.get());
		response2.rewind();
		
		gapRequestManager.processGapResponse(null, null, response2);
		Assert.assertTrue(gapRequestManager.isResponseHeaderReceived());
		Assert.assertEquals(3, gapRequestManager.getResponseFirstSequenceNumber());
		Assert.assertEquals(5, gapRequestManager.getResponsePacketCount());
		Assert.assertEquals(0, gapRequestManager.getPacketsRemainingToDeliver());
	}

	@Test
	public void testProcessGapResponseNone() throws IOException
	{
		TestSelectorThread selectorThread = new TestSelectorThread();
		ChannelReceiveInfo channelReceiveInfo = new ChannelReceiveInfo("1.1.1.1", 1, "1.1.1.1:1", "127.0.0.1", 10, selectorThread, 10000);
		ChannelReceiveSequencer sequencer = new ChannelReceiveSequencer(selectorThread, "key", "1.1.1.1:1", "127.0.0.2", channelReceiveInfo, 1000);
		GapRequestManager gapRequestManager = new GapRequestManager(selectorThread, "1.1.1.1:1", "127.0.0.1", sequencer);
		
		gapRequestManager.sendGapRequest(100, 3, 5);
		ByteBuffer response = createResponse(0, 0);
		
		gapRequestManager.processGapResponse(null, null, response);
		Assert.assertTrue(gapRequestManager.isResponseHeaderReceived());
		Assert.assertEquals(0, gapRequestManager.getResponseFirstSequenceNumber());
		Assert.assertEquals(0, gapRequestManager.getResponsePacketCount());
		Assert.assertEquals(0, gapRequestManager.getPacketsRemainingToDeliver());
	}

	@Test
	public void testProcessGapResponsePartial() throws IOException
	{
		TestSelectorThread selectorThread = new TestSelectorThread();
		ChannelReceiveInfo channelReceiveInfo = new ChannelReceiveInfo("1.1.1.1", 1, "1.1.1.1:1", "127.0.0.1", 10, selectorThread, 10000);
		ChannelReceiveSequencer sequencer = new ChannelReceiveSequencer(selectorThread, "key", "1.1.1.1:1", "127.0.0.2", channelReceiveInfo, 1000);
		GapRequestManager gapRequestManager = new GapRequestManager(selectorThread, "1.1.1.1:1", "127.0.0.1", sequencer);
		
		gapRequestManager.sendGapRequest(100, 3, 5);
		ByteBuffer response = createResponse(6, 2);
		
		gapRequestManager.processGapResponse(null, null, response);
		Assert.assertTrue(gapRequestManager.isResponseHeaderReceived());
		Assert.assertEquals(6, gapRequestManager.getResponseFirstSequenceNumber());
		Assert.assertEquals(2, gapRequestManager.getResponsePacketCount());
		Assert.assertEquals(0, gapRequestManager.getPacketsRemainingToDeliver());
	}

	private static ByteBuffer createResponse(int firstSequenceNumber, int packetCount)
	{		
		LinkedList<ByteBuffer> packetBuffers = new LinkedList<ByteBuffer>();
		int totalLength = 0;
		for(int i=0; i<packetCount; i++)
		{
			ByteBuffer buffer = createPacket(5, firstSequenceNumber+i);
			packetBuffers.add(buffer);
			totalLength += buffer.remaining();
		}
		
		int responseLength = PandaUtils.RETRANSMISSION_RESPONSE_HEADER_SIZE + PandaUtils.RETRANSMISSION_RESPONSE_PACKET_HEADER_SIZE*packetCount + totalLength;
		ByteBuffer buffer = ByteBuffer.allocate(responseLength);
		buffer.putLong(firstSequenceNumber);
		buffer.putInt(packetCount);
		for(ByteBuffer packetBuffer : packetBuffers)
		{
			buffer.putInt(packetBuffer.remaining());
			buffer.put(packetBuffer);
		}
		
		buffer.rewind();
		return buffer;
	}

	private static ByteBuffer createPacket(int messageCount, long sequenceNumber)
	{
		String topic = "1";
		ByteBuffer buffer = ByteBuffer.allocate(PandaUtils.PACKET_HEADER_SIZE + (PandaUtils.MESSAGE_HEADER_FIXED_SIZE + topic.length() + 4) * messageCount);
		buffer.put(PandaUtils.PACKET_HEADER_SIZE);
		buffer.put((byte) 1); // supports retrans
		buffer.putLong(sequenceNumber);
		buffer.put((byte) messageCount); 
		for(int i = 0; i < messageCount; i++)
		{
			buffer.put((byte)1);
			buffer.put(topic.getBytes());
			buffer.putShort((short) 4); // messageLength
			buffer.putInt(0);
		}
		buffer.rewind();
		return buffer;
	}
}