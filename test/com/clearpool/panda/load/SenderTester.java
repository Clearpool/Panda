package com.clearpool.panda.load;

import java.net.InetAddress;
import java.nio.ByteBuffer;

import com.clearpool.panda.core.PandaAdapter;
import com.clearpool.panda.core.PandaDataListener;
import com.clearpool.panda.core.PandaErrorCode;
import com.clearpool.panda.core.PandaTopicInfo;


public class SenderTester
{
	long messageSeqNum;
	long highestSeqNum;
	int returnedMessageCount;
	long startedRecvTimeStamp;
	long endRecvTimeStamp;

	long minRTLatency = Long.MAX_VALUE;
	long maxRTLatency = Long.MIN_VALUE;
	long latencyAccumulator = 0;

	// 24 <= dataSize <= Utils.MAX_MESSAGE_PAYLOAD_SIZE
	public void sendSequencedMessages(int cacheSize, PandaTopicInfo tInfo, int dataSize, final long numOfMessages, int netRecvBufferSize, int numOfSenderThreads, final String localIp) throws Exception
	{
		final PandaAdapter adapter = new PandaAdapter(cacheSize);
		final PandaTopicInfo topicInfo = tInfo;

		adapter.subscribe(topicInfo, localIp, new PandaDataListener() {
			private long recdThisSecondTimeStamp;
			private long currentTime;
			private int messagesTillLastSec;

			@Override
			public void receivedPandaData(String topic, ByteBuffer payload)
			{
				this.currentTime = System.currentTimeMillis();
				SenderTester.this.messageSeqNum = payload.getLong(8);
				long packetSentTimeStamp = payload.getLong(16);
				SenderTester.this.minRTLatency = Math.min(SenderTester.this.minRTLatency, (this.currentTime - packetSentTimeStamp));
				SenderTester.this.maxRTLatency = Math.max(SenderTester.this.maxRTLatency, (this.currentTime - packetSentTimeStamp));
				SenderTester.this.latencyAccumulator += (this.currentTime - packetSentTimeStamp);

				SenderTester.this.highestSeqNum = Math.max(SenderTester.this.highestSeqNum, SenderTester.this.messageSeqNum);
				++SenderTester.this.returnedMessageCount;

				if (SenderTester.this.startedRecvTimeStamp == 0)
				{
					SenderTester.this.startedRecvTimeStamp = System.currentTimeMillis();
					this.recdThisSecondTimeStamp = SenderTester.this.startedRecvTimeStamp;
				}

				if (this.currentTime > this.recdThisSecondTimeStamp + 1000)
				{
					this.recdThisSecondTimeStamp = this.currentTime;
					System.out.println("--- Recd. " + (SenderTester.this.returnedMessageCount - this.messagesTillLastSec) + " Messages In The Last Second. Total Messages Recd. " + SenderTester.this.returnedMessageCount);
					this.messagesTillLastSec = SenderTester.this.returnedMessageCount;
				}

				SenderTester.this.endRecvTimeStamp = System.currentTimeMillis();
			}

			@Override
			public void receivedPandaError(PandaErrorCode issueCode, String multicastGroup, String message, Throwable throable)
			{
				System.err.println(issueCode + "|" + multicastGroup + "|" + message);
			}
		}, netRecvBufferSize);

		SenderThread[] senderThreads = new SenderThread[numOfSenderThreads];
		long mssgsPerOtherThreads = numOfMessages / numOfSenderThreads;
		long mssgsPerFirstThread = (numOfMessages - (numOfSenderThreads - 1) * mssgsPerOtherThreads);

		senderThreads[0] = new SenderThread(adapter, topicInfo, dataSize, mssgsPerFirstThread, 0, localIp);
		senderThreads[0].start();
		for (int i = 1; i < numOfSenderThreads; ++i)
		{
			senderThreads[i] = new SenderThread(adapter, topicInfo, dataSize, mssgsPerOtherThreads, i, localIp);
			senderThreads[i].start();
		}

		for (int i = 0; i < numOfSenderThreads; ++i)
		{
			senderThreads[i].join(10000);
		}

		Thread.sleep(10000); // Give enough time for the receiving part of sender

		System.out.println("*** Recd. " + SenderTester.this.returnedMessageCount + " Messages In " + (this.endRecvTimeStamp - SenderTester.this.startedRecvTimeStamp) + " Milliseconds");
		System.out.println("*** Messages Lost = " + (numOfMessages - SenderTester.this.returnedMessageCount) + " (" + (100 * (float) (numOfMessages - SenderTester.this.returnedMessageCount) / numOfMessages) + " %)");
		System.out.println("*** Min Round-Trip Latency : " + SenderTester.this.minRTLatency + " Milliseconds -- Max Round-Trip Latency : " + SenderTester.this.maxRTLatency + " Milliseconds -- Average Round-Trip Latency : "
				+ ((float) SenderTester.this.latencyAccumulator / SenderTester.this.returnedMessageCount) + " Milliseconds\n");

	}

	public static void runSender(PandaAdapter adapter, PandaTopicInfo topicInfo, String localIp, int dataSize, long numOfMessages, int senderThreadNum) throws Exception
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
			adapter.send(topicInfo, localIp, buffer.array());
			if (seqNumber % 200 == 0)
			{
				Thread.sleep(1);
			}
		}
		System.out.println("*** Sender Thread " + senderThreadNum + " Sent " + numOfMessages + " Messages In " + (System.currentTimeMillis() - startSendTimeStamp) + " Milliseconds");

	}

	class SenderThread extends Thread
	{
		PandaAdapter pandaAdapter;
		PandaTopicInfo tInfo;
		int packetSize;
		long numOfMssgs;
		int thisThreadNum;
		String localIp;

		public SenderThread(PandaAdapter adapter, PandaTopicInfo topicInfo, int dataSize, long numOfMessages, int senderThreadNum, String localIp)
		{
			this.pandaAdapter = adapter;
			this.tInfo = topicInfo;
			this.packetSize = dataSize;
			this.numOfMssgs = numOfMessages;
			this.thisThreadNum = senderThreadNum;
			this.localIp = localIp;
		}

		@Override
		public void run()
		{
			try
			{
				SenderTester.runSender(this.pandaAdapter, this.tInfo, this.localIp, this.packetSize, this.numOfMssgs, this.thisThreadNum);
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}

	};

	public static void main(String[] args) throws Exception
	{
		if (args.length != 6)
		{
			System.out.println("*** usage -- panda.core.SenderTest2 (int)pandaAdapterCache (int)numOfMessages (int)payloadSize (int)netRecvBufferSize, (int) 1 <= numOfThreads <= 5");
			System.exit(0);
		}
		int adapterCache = Integer.valueOf(args[0]).intValue();
		int numOfMessages = Integer.valueOf(args[1]).intValue();
		int payloadSize = Integer.valueOf(args[2]).intValue();
		int netRecvBufferSize = Integer.valueOf(args[3]).intValue();
		int numOfThreads = Integer.valueOf(args[4]).intValue();
		if (numOfThreads < 1 || numOfThreads > 5)
		{
			System.out.println("*** Number of threads should be between 1 and 5");
			System.exit(0);
		}

		PandaTopicInfo topicInfo = new PandaTopicInfo("239.9.9.10", Integer.valueOf(9002), "TEST_TOPIC");
		String localIp = InetAddress.getLocalHost().getHostAddress();

		SenderTester st = new SenderTester();
		st.sendSequencedMessages(adapterCache, topicInfo, payloadSize, numOfMessages, netRecvBufferSize, numOfThreads, localIp);
		System.exit(0);
	}
}
