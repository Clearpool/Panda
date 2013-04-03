package panda.tester;

import java.net.InetAddress;
import java.nio.ByteBuffer;

import panda.core.Adapter;
import panda.core.containers.TopicInfo;


public class SenderTester
{
	private final long senderInitTimeStamp;
	
	public SenderTester() throws InterruptedException
	{
		this.senderInitTimeStamp = System.currentTimeMillis();
		Thread.sleep(5); // to ensure 2 different senders have different value
	}
	
	// 16 <= dataSize <= Utils.MAX_MESSAGE_PAYLOAD_SIZE
	public void SendSequencedMessages(int cacheSize, TopicInfo tInfo, int dataSize, long numOfMessages) throws Exception
	{
		final Adapter adapter = new Adapter(cacheSize);
		final TopicInfo topicInfo = tInfo;
		long seqNumber = 0;
		long mssgPerSecCounter = 0;
		
		
		long currentTimeStamp = 0;
		long startSendTimeStamp = System.currentTimeMillis();
		long secondCounter = startSendTimeStamp;
	
		while(seqNumber < numOfMessages)
		{
			ByteBuffer buffer = ByteBuffer.allocate(dataSize);
			buffer.putLong(0, this.senderInitTimeStamp); // for Sender Identification at app level
			buffer.putLong(8, ++seqNumber);
			
			if((currentTimeStamp = System.currentTimeMillis()) > secondCounter + 1000)
			{
				secondCounter = currentTimeStamp;
				System.out.println("Sent " + (seqNumber - mssgPerSecCounter) + " messages this second");
				mssgPerSecCounter = seqNumber;
			}
			adapter.send(topicInfo, InetAddress.getLocalHost().getHostAddress(), buffer.array());
		}
		System.out.println("Sent " + numOfMessages + " messages in " + (System.currentTimeMillis() - startSendTimeStamp) + " milliseconds");
		
	}
}
