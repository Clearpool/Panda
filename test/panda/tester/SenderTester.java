package panda.tester;

import java.net.InetAddress;
import java.nio.ByteBuffer;

import panda.core.IDataListener;
import panda.core.PandaAdapter;
import panda.core.containers.TopicInfo;

public class SenderTester
{
	private final long senderInitTimeStamp;

	public SenderTester() throws InterruptedException
	{
		this.senderInitTimeStamp = System.currentTimeMillis();
		Thread.sleep(5); // to ensure 2 different senders have different value
	}

	// 24 <= dataSize <= Utils.MAX_MESSAGE_PAYLOAD_SIZE
	public void SendSequencedMessages(int cacheSize, TopicInfo tInfo, int dataSize, final long numOfMessages, int netRecvBufferSize) throws Exception
	{
		final PandaAdapter adapter = new PandaAdapter(cacheSize);
		final TopicInfo topicInfo = tInfo;
		long seqNumber = 0;
		long mssgPerSecCounter = 0;

		long currentTimeStamp = 0;
		long startSendTimeStamp = System.currentTimeMillis();
		long secondCounter = startSendTimeStamp;

		adapter.subscribe(topicInfo, InetAddress.getLocalHost().getHostAddress(), new IDataListener() {
			private long messageSeqNum;
			private int returnedMessageCount;
			private long startedRecvTimeStamp;
			private long recdThisSecondTimeStamp;
			private long currentTime;
			private int messagesTillLastSec;
			private long endRecvTimeStamp;

			@Override
			public void receivedPandaData(int topicId, ByteBuffer payload)
			{
				this.messageSeqNum = payload.getLong(8);
				++this.returnedMessageCount;

				if (this.startedRecvTimeStamp == 0)
				{
					this.startedRecvTimeStamp = System.currentTimeMillis();
					this.recdThisSecondTimeStamp = this.startedRecvTimeStamp;
				}

				if ((this.currentTime = System.currentTimeMillis()) > this.recdThisSecondTimeStamp + 1000)
				{
					this.recdThisSecondTimeStamp = this.currentTime;
					System.out.println("Recd. " + (this.returnedMessageCount - this.messagesTillLastSec) + " messages in the last second. Total Messages Recd. " + this.returnedMessageCount);
					this.messagesTillLastSec = this.returnedMessageCount;
				}

				if (this.returnedMessageCount == numOfMessages)
				{
					this.endRecvTimeStamp = System.currentTimeMillis();
					System.out.println("Recd. " + this.returnedMessageCount + " in " + (this.endRecvTimeStamp - this.startedRecvTimeStamp) + " milliseconds. Recd. sender seq # " + this.messageSeqNum);
					System.exit(0);
				}
			}
		}, netRecvBufferSize
		);

		while ( true /*seqNumber < numOfMessages*/ )
		{
			ByteBuffer buffer = ByteBuffer.allocate(dataSize);
			buffer.putLong(0, this.senderInitTimeStamp); // for Sender
															// Identification at
															// app level
			buffer.putLong(8, ++seqNumber);
			buffer.putLong(16, System.currentTimeMillis());

			if ((currentTimeStamp = System.currentTimeMillis()) > secondCounter + 1000)
			{
				secondCounter = currentTimeStamp;
				System.out.println("Sent " + (seqNumber - mssgPerSecCounter) + " messages this second");
				mssgPerSecCounter = seqNumber;
			}
			adapter.send(topicInfo, InetAddress.getLocalHost().getHostAddress(), buffer.array());
			if(seqNumber % 400 == 0)Thread.sleep(1);
		}
		// System.out.println("Sent " + numOfMessages + " messages in " +
		// (System.currentTimeMillis() - startSendTimeStamp) + " milliseconds");

	}
}
