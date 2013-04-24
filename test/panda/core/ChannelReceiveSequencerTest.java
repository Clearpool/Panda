package panda.core;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Assert;

import org.junit.Test;


@SuppressWarnings("static-method")
public class ChannelReceiveSequencerTest
{	
	@Test
	public void testSkipPacketAndDequeue() throws IOException
	{
		TestSelectorThread testSelectorThread = new TestSelectorThread();
		ChannelReceiveInfo testChannelReceiveInfo = new ChannelReceiveInfo("test1:1000", 1000, "test1:1000", "127.0.0.1", 1, testSelectorThread, 1000);
		ChannelReceiveSequencer sequencer = new ChannelReceiveSequencer(testSelectorThread, "test", "test1:1000", "test0", testChannelReceiveInfo, 100);
		
		//[1,1] - No Queue
		sequencer.packetReceived(true, 100, 1, (byte)3, createPacket(3));
		Assert.assertEquals(1, sequencer.getLastSequenceNumber());
		
		//[3,10] - Queued
		for(int i=3;i<=10;i++)
		{
			sequencer.packetReceived(true, 100, i, (byte)3, createPacket(3));
		}
		Assert.assertEquals(8, sequencer.getQueueSize());
		Assert.assertNull(sequencer.getGapRequestManager());
		Assert.assertEquals(1, sequencer.getLastSequenceNumber());
		
		//Declare drop
		sequencer.skipPacketAndDequeue(2);
		Assert.assertEquals(0, sequencer.getQueueSize());
		Assert.assertNull(sequencer.getGapRequestManager());
		Assert.assertEquals(10, sequencer.getLastSequenceNumber());
		Assert.assertEquals(1, sequencer.getPacketsLost());
		Assert.assertEquals(0, sequencer.getPacketsDropped());
	}
	
	@Test
	public void testSendGapRequest() throws IOException, InterruptedException
	{
		TestSelectorThread testSelectorThread = new TestSelectorThread();
		ChannelReceiveInfo testChannelReceiveInfo = new ChannelReceiveInfo("test1:1000", 1000, "test1:1000", "127.0.0.1", 1, testSelectorThread, 1000);
		ChannelReceiveSequencer sequencer = new ChannelReceiveSequencer(testSelectorThread, "test", "test1:1000", "test0", testChannelReceiveInfo, 100);
		
		//[1,1] - No Queue
		sequencer.packetReceived(true, 100, 1, (byte)3, createPacket(3));
		Assert.assertEquals(1, sequencer.getLastSequenceNumber());
		
		//[3,22] - Queued
		for(int i=3;i<=22;i++)
		{
			sequencer.packetReceived(true, 100, i, (byte)3, createPacket(3));
		}
		Assert.assertEquals(20, sequencer.getQueueSize());
		Assert.assertNull(sequencer.getGapRequestManager());
		Assert.assertEquals(1, sequencer.getLastSequenceNumber());
		
		//[23,23] - Will cause threshold to be crossed
		sequencer.packetReceived(true, 100, 23, (byte)3, createPacket(3));
		Assert.assertEquals(21, sequencer.getQueueSize());
		Assert.assertNotNull(sequencer.getGapRequestManager());
		Assert.assertEquals(1, sequencer.getLastSequenceNumber());
		Assert.assertEquals(sequencer.getGapRequestManager().getFirstSequenceNumberRequested(), 2);
		Assert.assertEquals(sequencer.getGapRequestManager().getPacketCountRequested(), 1);
		
		//[24,24] - Add to queue while recovering
		sequencer.packetReceived(true, 100, 24, (byte)3, createPacket(3));
		Assert.assertEquals(22, sequencer.getQueueSize());
		Assert.assertEquals(sequencer.getGapRequestManager().getFirstSequenceNumberRequested(), 2);
		Assert.assertEquals(sequencer.getGapRequestManager().getPacketCountRequested(), 1);
		long timeOfRequest = sequencer.getGapRequestManager().getTimeOfRequest();
		
		//[26,50] - Add to queue while recovering
		for(int i=26;i<=50;i++)
		{
			sequencer.packetReceived(true, 100, i, (byte)3, createPacket(3));
		}
		Assert.assertEquals(47, sequencer.getQueueSize());
		Assert.assertEquals(sequencer.getGapRequestManager().getFirstSequenceNumberRequested(), 2);
		Assert.assertEquals(sequencer.getGapRequestManager().getPacketCountRequested(), 1);
		Assert.assertEquals(timeOfRequest, sequencer.getGapRequestManager().getTimeOfRequest());
		
		//declare 2 as dropped
		sequencer.closeRequestManager(false);
		sequencer.skipPacketAndDequeue(2);
		Assert.assertEquals(24, sequencer.getLastSequenceNumber());
		Assert.assertEquals(25, sequencer.getQueueSize());
		Assert.assertNull(sequencer.getGapRequestManager());
		
		Thread.sleep(1);
		
		//[51,51] - Add to queue while recovering
		sequencer.packetReceived(true, 100, 51, (byte)3, createPacket(3));
		Assert.assertEquals(26, sequencer.getQueueSize());
		Assert.assertEquals(sequencer.getGapRequestManager().getFirstSequenceNumberRequested(), 25);
		Assert.assertEquals(sequencer.getGapRequestManager().getPacketCountRequested(), 1);
		Assert.assertTrue(sequencer.getGapRequestManager().getTimeOfRequest() > timeOfRequest);
	}
	
	@Test
	public void testPacketReceivedNoDrops() throws IOException
	{
		TestSelectorThread testSelectorThread = new TestSelectorThread();
		ChannelReceiveInfo testChannelReceiveInfo = new ChannelReceiveInfo("test1:1000", 1000, "test1:1000", "127.0.0.1", 1, testSelectorThread, 1000);
		ChannelReceiveSequencer sequencer = new ChannelReceiveSequencer(testSelectorThread, "test", "test1:1000", "test0", testChannelReceiveInfo, 100);
		
		for(int i=1; i<=1000; i++)
		{
			sequencer.packetReceived(true, 100, i, (byte)3, createPacket(3));
		}
		Assert.assertEquals(0, sequencer.getQueueSize());
		Assert.assertNull(sequencer.getGapRequestManager());
		Assert.assertEquals(1000, sequencer.getLastSequenceNumber());
		Assert.assertEquals(0, sequencer.getPacketsLost());
		Assert.assertEquals(0, sequencer.getPacketsDropped());
	}
	
	@Test
	public void testPacketReceivedSelfCorrectingOutOfSequence() throws IOException
	{
		TestSelectorThread testSelectorThread = new TestSelectorThread();
		ChannelReceiveInfo testChannelReceiveInfo = new ChannelReceiveInfo("test1:1000", 1000, "test1:1000", "127.0.0.1", 1, testSelectorThread, 1000);
		ChannelReceiveSequencer sequencer = new ChannelReceiveSequencer(testSelectorThread, "test", "test1:1000", "test0", testChannelReceiveInfo, 100);
		
		//[1,1] - No Queue
		sequencer.packetReceived(true, 100, 1, (byte)3, createPacket(3));
		Assert.assertEquals(1, sequencer.getLastSequenceNumber());
		
		//[3,10] - Queued
		for(int i=3;i<=10;i++)
		{
			sequencer.packetReceived(true, 100, i, (byte)3, createPacket(3));
		}
		Assert.assertEquals(8, sequencer.getQueueSize());
		Assert.assertNull(sequencer.getGapRequestManager());
		Assert.assertEquals(1, sequencer.getLastSequenceNumber());
		
		//[2,2] - Fixed
		sequencer.packetReceived(true, 100, 2, (byte)3, createPacket(3));
		Assert.assertEquals(0, sequencer.getQueueSize());
		Assert.assertNull(sequencer.getGapRequestManager());
		Assert.assertEquals(10, sequencer.getLastSequenceNumber());
		Assert.assertEquals(0, sequencer.getPacketsLost());
		Assert.assertEquals(0, sequencer.getPacketsDropped());
	}
	
	@Test
	public void testPacketReceivedOutofSequenceThresholdNoRetransSupport() throws IOException
	{
		TestSelectorThread testSelectorThread = new TestSelectorThread();
		ChannelReceiveInfo testChannelReceiveInfo = new ChannelReceiveInfo("test1:1000", 1000, "test1:1000", "127.0.0.1", 1, testSelectorThread, 1000);
		ChannelReceiveSequencer sequencer = new ChannelReceiveSequencer(testSelectorThread, "test", "test1:1000", "test0", testChannelReceiveInfo, 100);
		
		//[1,1] - No Queue
		sequencer.packetReceived(false, 100, 1, (byte)3, createPacket(3));
		Assert.assertEquals(1, sequencer.getLastSequenceNumber());
		
		//[3,22] - Queued
		for(int i=3;i<=22;i++)
		{
			sequencer.packetReceived(false, 100, i, (byte)3, createPacket(3));
		}
		Assert.assertEquals(20, sequencer.getQueueSize());
		Assert.assertNull(sequencer.getGapRequestManager());
		Assert.assertEquals(1, sequencer.getLastSequenceNumber());
		
		//[23,23] - Will cause threshold to be crossed
		sequencer.packetReceived(false, 100, 23, (byte)3, createPacket(3));
		Assert.assertEquals(23, sequencer.getLastSequenceNumber());
		Assert.assertEquals(1, sequencer.getPacketsLost());
		Assert.assertEquals(1, sequencer.getPacketsDropped());
	}
	
	@Test
	public void testPacketReceivedOutofSequenceThresholdWithRetransSupport() throws IOException
	{
		TestSelectorThread testSelectorThread = new TestSelectorThread();
		ChannelReceiveInfo testChannelReceiveInfo = new ChannelReceiveInfo("test1:1000", 1000, "test1:1000", "127.0.0.1", 1, testSelectorThread, 1000);
		ChannelReceiveSequencer sequencer = new ChannelReceiveSequencer(testSelectorThread, "test", "test1:1000", "test0", testChannelReceiveInfo, 100);
		
		//[1,1] - No Queue
		sequencer.packetReceived(true, 100, 1, (byte)3, createPacket(3));
		Assert.assertEquals(1, sequencer.getLastSequenceNumber());
		
		//[3,22] - Queued
		for(int i=3;i<=22;i++)
		{
			sequencer.packetReceived(true, 100, i, (byte)3, createPacket(3));
		}
		Assert.assertEquals(20, sequencer.getQueueSize());
		Assert.assertNull(sequencer.getGapRequestManager());
		Assert.assertEquals(1, sequencer.getLastSequenceNumber());
		
		//[23,23] - Will cause threshold to be crossed
		sequencer.packetReceived(true, 100, 23, (byte)3, createPacket(3));
		Assert.assertEquals(21, sequencer.getQueueSize());
		Assert.assertNotNull(sequencer.getGapRequestManager());
		Assert.assertEquals(1, sequencer.getLastSequenceNumber());
		Assert.assertEquals(sequencer.getGapRequestManager().getFirstSequenceNumberRequested(), 2);
		Assert.assertEquals(sequencer.getGapRequestManager().getPacketCountRequested(), 1);
		Assert.assertEquals(0, sequencer.getPacketsLost());
		Assert.assertEquals(1, sequencer.getPacketsDropped());
	}
	
	@Test
	public void testPacketReceivedQueueGiveUpTimeNoRetransSupport() throws IOException, InterruptedException
	{
		TestSelectorThread testSelectorThread = new TestSelectorThread();
		ChannelReceiveInfo testChannelReceiveInfo = new ChannelReceiveInfo("test1:1000", 1000, "test1:1000", "127.0.0.1", 1, testSelectorThread, 1000);
		ChannelReceiveSequencer sequencer = new ChannelReceiveSequencer(testSelectorThread, "test", "test1:1000", "test0", testChannelReceiveInfo, 100);
		
		//[1,1] - No Queue
		sequencer.packetReceived(false, 100, 1, (byte)3, createPacket(3));
		Assert.assertEquals(1, sequencer.getLastSequenceNumber());
		
		//[3,10] - Queued
		for(int i=3;i<=10;i++)
		{
			sequencer.packetReceived(false, 100, i, (byte)3, createPacket(3));
		}
		Assert.assertEquals(8, sequencer.getQueueSize());
		Assert.assertNull(sequencer.getGapRequestManager());
		Assert.assertEquals(1, sequencer.getLastSequenceNumber());
		
		Thread.sleep(2050);
		
		//[11] - Will cause threshold to be crossed
		sequencer.packetReceived(false, 100, 11, (byte)3, createPacket(3));
		Assert.assertEquals(0, sequencer.getQueueSize());
		Assert.assertNull(sequencer.getGapRequestManager());
		Assert.assertEquals(11, sequencer.getLastSequenceNumber());
		Assert.assertEquals(1, sequencer.getPacketsLost());
		Assert.assertEquals(1, sequencer.getPacketsDropped());
	}
	
	@Test
	public void testPacketReceivedQueueGiveUpTimeWithRetransSupport() throws IOException, InterruptedException
	{
		TestSelectorThread testSelectorThread = new TestSelectorThread();
		ChannelReceiveInfo testChannelReceiveInfo = new ChannelReceiveInfo("test1:1000", 1000, "test1:1000", "127.0.0.1", 1, testSelectorThread, 1000);
		ChannelReceiveSequencer sequencer = new ChannelReceiveSequencer(testSelectorThread, "test", "test1:1000", "test0", testChannelReceiveInfo, 100);
		
		//[1,1] - No Queue
		sequencer.packetReceived(true, 100, 1, (byte)3, createPacket(3));
		Assert.assertEquals(1, sequencer.getLastSequenceNumber());
		
		//[3,10] - Queued
		for(int i=3;i<=10;i++)
		{
			sequencer.packetReceived(true, 100, i, (byte)3, createPacket(3));
		}
		Assert.assertEquals(8, sequencer.getQueueSize());
		Assert.assertNull(sequencer.getGapRequestManager());
		Assert.assertEquals(1, sequencer.getLastSequenceNumber());
		
		Thread.sleep(2050);
		
		//[11] - Will cause threshold to be crossed
		sequencer.packetReceived(true, 100, 11, (byte)3, createPacket(3));
		Assert.assertEquals(9, sequencer.getQueueSize());
		Assert.assertNotNull(sequencer.getGapRequestManager());
		Assert.assertEquals(1, sequencer.getLastSequenceNumber());
		Assert.assertEquals(sequencer.getGapRequestManager().getFirstSequenceNumberRequested(), 2);
		Assert.assertEquals(sequencer.getGapRequestManager().getPacketCountRequested(), 1);
		Assert.assertEquals(0, sequencer.getPacketsLost());
		Assert.assertEquals(1, sequencer.getPacketsDropped());
	}
	
	@Test
	public void testPacketReceivedSenderRestart() throws IOException
	{
		TestSelectorThread testSelectorThread = new TestSelectorThread();
		ChannelReceiveInfo testChannelReceiveInfo = new ChannelReceiveInfo("test1:1000", 1000, "test1:1000", "127.0.0.1", 1, testSelectorThread, 1000);
		ChannelReceiveSequencer sequencer = new ChannelReceiveSequencer(testSelectorThread, "test", "test1:1000", "test0", testChannelReceiveInfo, 100);
		
		//Scenario 1 - restart after only receiving 1 packet
		sequencer.packetReceived(false, 100, 1, (byte)3, createPacket(3));
		Assert.assertEquals(1, sequencer.getLastSequenceNumber());
		
		sequencer.packetReceived(false, 100, 1, (byte)3, createPacket(3));
		Assert.assertEquals(1, sequencer.getLastSequenceNumber());
		Assert.assertEquals(0, sequencer.getQueueSize());
		Assert.assertEquals(0, sequencer.getPacketsLost());
		Assert.assertEquals(0, sequencer.getPacketsDropped());
		
		//Scenario 2 - restart after only receiving 2 packets
		sequencer.packetReceived(false, 100, 1, (byte)3, createPacket(3));
		Assert.assertEquals(1, sequencer.getLastSequenceNumber());
		sequencer.packetReceived(false, 100, 2, (byte)3, createPacket(3));
		Assert.assertEquals(2, sequencer.getLastSequenceNumber());
		
		sequencer.packetReceived(false, 100, 1, (byte)3, createPacket(3));
		Assert.assertEquals(1, sequencer.getLastSequenceNumber());
		Assert.assertEquals(0, sequencer.getQueueSize());
		Assert.assertEquals(0, sequencer.getPacketsLost());
		Assert.assertEquals(0, sequencer.getPacketsDropped());
		
		//Scenario 3 - restart after having queued data
		sequencer.packetReceived(false, 100, 1, (byte)3, createPacket(3));
		Assert.assertEquals(1, sequencer.getLastSequenceNumber());
		sequencer.packetReceived(false, 100, 3, (byte)3, createPacket(3));
		Assert.assertEquals(1, sequencer.getLastSequenceNumber());
		Assert.assertEquals(1, sequencer.getQueueSize());
		
		sequencer.packetReceived(false, 100, 1, (byte)3, createPacket(3));
		Assert.assertEquals(1, sequencer.getLastSequenceNumber());
		Assert.assertEquals(0, sequencer.getQueueSize());
		Assert.assertEquals(0, sequencer.getPacketsLost());
		Assert.assertEquals(0, sequencer.getPacketsDropped());
	}
	
	@Test
	public void testPacketReceivedMaxDroppedPacketsAllowed() throws IOException
	{
		TestSelectorThread testSelectorThread = new TestSelectorThread();
		ChannelReceiveInfo testChannelReceiveInfo = new ChannelReceiveInfo("test1:1000", 1000, "test1:1000", "127.0.0.1", 1, testSelectorThread, 1000);
		ChannelReceiveSequencer sequencer = new ChannelReceiveSequencer(testSelectorThread, "test", "test1:1000", "test0", testChannelReceiveInfo, 2);
		
		//[1,1] - No Queue
		sequencer.packetReceived(false, 100, 1, (byte)3, createPacket(3));
		Assert.assertEquals(1, sequencer.getLastSequenceNumber());
		
		//[3,22] - Queued
		for(int i=3;i<=22;i++)
		{
			sequencer.packetReceived(false, 100, i, (byte)3, createPacket(3));
		}
		Assert.assertEquals(20, sequencer.getQueueSize());
		Assert.assertNull(sequencer.getGapRequestManager());
		Assert.assertEquals(1, sequencer.getLastSequenceNumber());
		Assert.assertEquals(0, sequencer.getPacketsDropped());
		
		//[23,23] - Queued - causes drop
		sequencer.packetReceived(false, 100, 23, (byte)3, createPacket(3));
		Assert.assertEquals(23, sequencer.getLastSequenceNumber());
		Assert.assertEquals(1, sequencer.getPacketsDropped());
		
		//[25,44] - Queued
		for(int i=25;i<=44;i++)
		{
			sequencer.packetReceived(false, 100, i, (byte)3, createPacket(3));
		}
		Assert.assertEquals(20, sequencer.getQueueSize());
		Assert.assertNull(sequencer.getGapRequestManager());
		Assert.assertEquals(23, sequencer.getLastSequenceNumber());
		Assert.assertEquals(1, sequencer.getPacketsDropped());
		
		//[45,45] - Queued - causes drop
		sequencer.packetReceived(false, 100, 45, (byte)3, createPacket(3));
		Assert.assertEquals(45, sequencer.getLastSequenceNumber());
		Assert.assertEquals(2, sequencer.getPacketsDropped());
		
		//[47,47] - Queued - causes drop
		sequencer.packetReceived(false, 100, 47, (byte)3, createPacket(3));
		Assert.assertEquals(47, sequencer.getLastSequenceNumber());
		Assert.assertEquals(3, sequencer.getPacketsDropped());
		
		//[49,49] - Queued - causes drop
		sequencer.packetReceived(false, 100, 49, (byte)3, createPacket(3));
		Assert.assertEquals(49, sequencer.getLastSequenceNumber());
		Assert.assertEquals(4, sequencer.getPacketsDropped());
	}
	
	@Test
	public void testPacketReceivedDisabledRetransmissions() throws IOException
	{
		TestSelectorThread testSelectorThread = new TestSelectorThread();
		ChannelReceiveInfo testChannelReceiveInfo = new ChannelReceiveInfo("test1:1000", 1000, "test1:1000", "127.0.0.1", 1, testSelectorThread, 1000);
		ChannelReceiveSequencer sequencer = new ChannelReceiveSequencer(testSelectorThread, "test", "test1:1000", "test0", testChannelReceiveInfo, 100);
		
		//[1,1] - No Queue
		sequencer.packetReceived(true, 100, 1, (byte)3, createPacket(3));
		Assert.assertEquals(1, sequencer.getLastSequenceNumber());
		
		//Disable retransmissions
		sequencer.disableRetransmissions();
		
		//[3,22] - Queued
		for(int i=3;i<=22;i++)
		{
			sequencer.packetReceived(true, 100, i, (byte)3, createPacket(3));
		}
		Assert.assertEquals(20, sequencer.getQueueSize());
		Assert.assertNull(sequencer.getGapRequestManager());
		Assert.assertEquals(1, sequencer.getLastSequenceNumber());
		Assert.assertEquals(0, sequencer.getPacketsDropped());
		
		//[23,23] - Queued - causes drop
		sequencer.packetReceived(true, 100, 23, (byte)3, createPacket(3));
		Assert.assertEquals(23, sequencer.getLastSequenceNumber());
		Assert.assertEquals(1, sequencer.getPacketsDropped());
		Assert.assertEquals(1, sequencer.getPacketsLost());
		Assert.assertNull(sequencer.getGapRequestManager());
	}
	
	@Test
	public void testPacketReceivedRetransmissionTimeout() throws IOException, InterruptedException
	{
		TestSelectorThread testSelectorThread = new TestSelectorThread();
		ChannelReceiveInfo testChannelReceiveInfo = new ChannelReceiveInfo("test1:1000", 1000, "test1:1000", "127.0.0.1", 1, testSelectorThread, 1000);
		ChannelReceiveSequencer sequencer = new ChannelReceiveSequencer(testSelectorThread, "test", "test1:1000", "test0", testChannelReceiveInfo, 100);
		
		//[1,1] - No Queue
		sequencer.packetReceived(true, 100, 1, (byte)3, createPacket(3));
		Assert.assertEquals(1, sequencer.getLastSequenceNumber());
		
		//[3,22] - Queued
		for(int i=3;i<=22;i++)
		{
			sequencer.packetReceived(true, 100, i, (byte)3, createPacket(3));
		}
		Assert.assertEquals(20, sequencer.getQueueSize());
		Assert.assertNull(sequencer.getGapRequestManager());
		Assert.assertEquals(1, sequencer.getLastSequenceNumber());
		Assert.assertEquals(0, sequencer.getPacketsDropped());
		
		//[23,23] - Queued - causes drop
		sequencer.packetReceived(true, 100, 23, (byte)3, createPacket(3));
		Assert.assertEquals(1, sequencer.getLastSequenceNumber());
		Assert.assertEquals(1, sequencer.getPacketsDropped());
		Assert.assertEquals(0, sequencer.getPacketsLost());
		Assert.assertNotNull(sequencer.getGapRequestManager());
		Assert.assertEquals(sequencer.getGapRequestManager().getFirstSequenceNumberRequested(), 2);
		Assert.assertEquals(sequencer.getGapRequestManager().getPacketCountRequested(), 1);
		
		Thread.sleep(2050);
		
		//[24,24] - Queued - causes retranmission to be cancelled and lost
		sequencer.packetReceived(true, 100, 24, (byte)3, createPacket(3));
		Assert.assertEquals(24, sequencer.getLastSequenceNumber());
		Assert.assertEquals(1, sequencer.getPacketsDropped());
		Assert.assertEquals(1, sequencer.getPacketsLost());
		Assert.assertNull(sequencer.getGapRequestManager());
	}
	
	@Test
	public void testPacketReceivedRetransmissionFailed() throws IOException
	{
		TestSelectorThread testSelectorThread = new TestSelectorThread();
		ChannelReceiveInfo testChannelReceiveInfo = new ChannelReceiveInfo("test1:1000", 1000, "test1:1000", "127.0.0.1", 1, testSelectorThread, 1000);
		ChannelReceiveSequencer sequencer = new ChannelReceiveSequencer(testSelectorThread, "test", "test1:1000", "test0", testChannelReceiveInfo, 100);
		
		//[1,1] - No Queue
		sequencer.packetReceived(true, 100, 1, (byte)3, createPacket(3));
		Assert.assertEquals(1, sequencer.getLastSequenceNumber());
		
		//[3,22] - Queued
		for(int i=3;i<=22;i++)
		{
			sequencer.packetReceived(true, 100, i, (byte)3, createPacket(3));
		}
		Assert.assertEquals(20, sequencer.getQueueSize());
		Assert.assertNull(sequencer.getGapRequestManager());
		Assert.assertEquals(1, sequencer.getLastSequenceNumber());
		Assert.assertEquals(0, sequencer.getPacketsDropped());
		
		//[23,23] - Queued - causes retrans 1
		sequencer.packetReceived(true, 100, 23, (byte)3, createPacket(3));
		Assert.assertEquals(1, sequencer.getLastSequenceNumber());
		Assert.assertEquals(1, sequencer.getPacketsDropped());
		Assert.assertEquals(0, sequencer.getPacketsLost());
		Assert.assertNotNull(sequencer.getGapRequestManager());
		Assert.assertEquals(sequencer.getGapRequestManager().getFirstSequenceNumberRequested(), 2);
		Assert.assertEquals(sequencer.getGapRequestManager().getPacketCountRequested(), 1);
		sequencer.getGapRequestManager().close(false);
		
		//[24,24] - Queued - causes retrans 2
		sequencer.packetReceived(true, 100, 24, (byte)3, createPacket(3));
		Assert.assertEquals(1, sequencer.getLastSequenceNumber());
		Assert.assertEquals(1, sequencer.getPacketsDropped());
		Assert.assertEquals(0, sequencer.getPacketsLost());
		Assert.assertNotNull(sequencer.getGapRequestManager());
		Assert.assertEquals(sequencer.getGapRequestManager().getFirstSequenceNumberRequested(), 2);
		Assert.assertEquals(sequencer.getGapRequestManager().getPacketCountRequested(), 1);
		sequencer.getGapRequestManager().close(false);
		
		//[25,25] - Queued - causes retrans 3
		sequencer.packetReceived(true, 100, 25, (byte)3, createPacket(3));
		Assert.assertEquals(1, sequencer.getLastSequenceNumber());
		Assert.assertEquals(1, sequencer.getPacketsDropped());
		Assert.assertEquals(0, sequencer.getPacketsLost());
		Assert.assertNotNull(sequencer.getGapRequestManager());
		Assert.assertEquals(sequencer.getGapRequestManager().getFirstSequenceNumberRequested(), 2);
		Assert.assertEquals(sequencer.getGapRequestManager().getPacketCountRequested(), 1);
		sequencer.getGapRequestManager().close(false);
		
		//[26,26] - Queued - causes skip
		sequencer.packetReceived(true, 100, 26, (byte)3, createPacket(3));
		Assert.assertEquals(26, sequencer.getLastSequenceNumber());
		Assert.assertEquals(1, sequencer.getPacketsDropped());
		Assert.assertEquals(1, sequencer.getPacketsLost());
		Assert.assertNull(sequencer.getGapRequestManager());
	}

	private static ByteBuffer createPacket(int messageCount)
	{
		ByteBuffer buffer = ByteBuffer.allocate(messageCount*(Utils.MESSAGE_HEADER_SIZE + 4)); //4 is the payloadsize
		for(int i=0; i<messageCount; i++)
		{
			buffer.putInt(i); //topicId
			buffer.putShort((short)4);
			buffer.putInt(i);
		}
		buffer.rewind();
		return buffer;
	}
}