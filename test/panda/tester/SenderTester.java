package panda.tester;

import java.net.InetAddress;
import java.nio.ByteBuffer;

import panda.core.IDataListener;
import panda.core.PandaAdapter;
import panda.core.containers.TopicInfo;

public class SenderTester
{
	public SenderTester()
	{
		// this.senderInitTimeStamp = System.currentTimeMillis();
		// Thread.sleep(5); // to ensure 2 different senders have different
		// value
	}

	public static long max(long a, long b)
	{
		if (a > b)
			return a;
		return b;
	}

	public static long min(long a, long b)
	{
		if (a < b)
			return a;
		return b;
	}

	long messageSeqNum;
	long highestSeqNum;
	int returnedMessageCount;
	long startedRecvTimeStamp;
	long endRecvTimeStamp;

	long minRTLatency = Long.MAX_VALUE;
	long maxRTLatency = Long.MIN_VALUE;
	long latencyAccumulator = 0;

	// 24 <= dataSize <= Utils.MAX_MESSAGE_PAYLOAD_SIZE
	public void SendSequencedMessages(int cacheSize, TopicInfo tInfo, int dataSize, final long numOfMessages, int netRecvBufferSize, int numOfSenderThreads) throws Exception
	{
		final PandaAdapter adapter = new PandaAdapter(cacheSize);
		final TopicInfo topicInfo = tInfo;

		adapter.subscribe(topicInfo, InetAddress.getLocalHost().getHostAddress(), new IDataListener() {

			private long recdThisSecondTimeStamp;
			private long currentTime;
			private int messagesTillLastSec;

			@Override
			public void receivedPandaData(int topicId, ByteBuffer payload)
			{
				this.currentTime = System.currentTimeMillis();
				SenderTester.this.messageSeqNum = payload.getLong(8);
				long packetSentTimeStamp = payload.getLong(16);
				SenderTester.this.minRTLatency = SenderTester.min(SenderTester.this.minRTLatency, (this.currentTime - packetSentTimeStamp));
				SenderTester.this.maxRTLatency = SenderTester.max(SenderTester.this.maxRTLatency, (this.currentTime - packetSentTimeStamp));
				SenderTester.this.latencyAccumulator += (this.currentTime - packetSentTimeStamp);

				SenderTester.this.highestSeqNum = SenderTester.max(SenderTester.this.highestSeqNum, SenderTester.this.messageSeqNum);
				++SenderTester.this.returnedMessageCount;

				if (SenderTester.this.startedRecvTimeStamp == 0)
				{
					SenderTester.this.startedRecvTimeStamp = System.currentTimeMillis();
					this.recdThisSecondTimeStamp = SenderTester.this.startedRecvTimeStamp;
				}

				if (this.currentTime > this.recdThisSecondTimeStamp + 1000)
				{
					this.recdThisSecondTimeStamp = this.currentTime;
					System.out.println("--- Recd. " + (SenderTester.this.returnedMessageCount - this.messagesTillLastSec) + " Messages In The Last Second. Total Messages Recd. "
							+ SenderTester.this.returnedMessageCount);
					this.messagesTillLastSec = SenderTester.this.returnedMessageCount;
				}

				SenderTester.this.endRecvTimeStamp = System.currentTimeMillis();
			}
		}, netRecvBufferSize);

		SenderThread[] senderThreads = new SenderThread[numOfSenderThreads];		
		long mssgsPerOtherThreads = numOfMessages / numOfSenderThreads;
		long mssgsPerFirstThread = (numOfMessages - (numOfSenderThreads - 1) * mssgsPerOtherThreads);

		senderThreads[0] = new SenderThread(adapter, topicInfo, dataSize, mssgsPerFirstThread, 0);
		senderThreads[0].start();
		for (int i = 1; i < numOfSenderThreads; ++i)
		{
			senderThreads[i] = new SenderThread(adapter, topicInfo, dataSize, mssgsPerOtherThreads, i);
			senderThreads[i].start();
		}
		
		for (int i = 0; i < numOfSenderThreads; ++i)
		{
			senderThreads[i].join(10000);
		}

		Thread.sleep(10000); // Give enough time for the receiving part of sender

		System.out.println("*** Recd. " + SenderTester.this.returnedMessageCount + " Messages In " + (this.endRecvTimeStamp - SenderTester.this.startedRecvTimeStamp) + " Milliseconds");
		System.out.println("*** Messages Lost = " + (numOfMessages - SenderTester.this.returnedMessageCount) + " ("
				+ (100 * (float) (numOfMessages - SenderTester.this.returnedMessageCount) / numOfMessages) + " %)");
		System.out.println("*** Min Round-Trip Latency : " + SenderTester.this.minRTLatency + " Milliseconds -- Max Round-Trip Latency : " + SenderTester.this.maxRTLatency
				+ " Milliseconds -- Average Round-Trip Latency : " + ((float) SenderTester.this.latencyAccumulator / SenderTester.this.returnedMessageCount) + " Milliseconds\n");

	}

	public static void runSender(PandaAdapter adapter, TopicInfo topicInfo, int dataSize, long numOfMessages, int senderThreadNum) throws Exception
	{
		long seqNumber = 0;
		long mssgPerSecCounter = 0;
		long currentTimeStamp = 0;
		long startSendTimeStamp = System.currentTimeMillis();
		long secondCounter = startSendTimeStamp;
		while (seqNumber < numOfMessages)
		{
			ByteBuffer buffer = ByteBuffer.allocate(dataSize);
			buffer.putLong(8, ++seqNumber);
			buffer.putLong(16, System.currentTimeMillis());

			if ((currentTimeStamp = System.currentTimeMillis()) > secondCounter + 1000)
			{
				secondCounter = currentTimeStamp;
				System.out.println("--- Sender Thread " + senderThreadNum + " Sent " + (seqNumber - mssgPerSecCounter) + " Messages This second. Total Messages Sent " + seqNumber);
				mssgPerSecCounter = seqNumber;
			}
			adapter.send(topicInfo, InetAddress.getLocalHost().getHostAddress(), buffer.array());
			if (seqNumber % 400 == 0)
			{
				Thread.sleep(1);
			}
		}
		System.out.println("*** Sender Thread " + senderThreadNum + " Sent " + numOfMessages + " Messages In " + (System.currentTimeMillis() - startSendTimeStamp) + " Milliseconds");

	}

	class SenderThread extends Thread
	{
		PandaAdapter pandaAdapter;
		TopicInfo tInfo;
		int packetSize;
		long numOfMssgs;
		int thisThreadNum;

		public SenderThread(PandaAdapter adapter, TopicInfo topicInfo, int dataSize, long numOfMessages, int senderThreadNum)
		{
			this.pandaAdapter = adapter;
			this.tInfo = topicInfo;
			this.packetSize = dataSize;
			this.numOfMssgs = numOfMessages;
			this.thisThreadNum = senderThreadNum;
		}

		@Override
		public void run()
		{
			try
			{
				SenderTester.runSender(this.pandaAdapter, this.tInfo, this.packetSize, this.numOfMssgs, this.thisThreadNum);
			}
			catch (Exception e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	};

}
