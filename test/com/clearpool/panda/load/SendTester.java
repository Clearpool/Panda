package com.clearpool.panda.load;

import java.net.InetAddress;
import java.nio.ByteBuffer;

import com.clearpool.panda.core.PandaAdapter;
import com.clearpool.panda.core.PandaDataListener;
import com.clearpool.panda.core.PandaErrorCode;
import com.clearpool.panda.core.PandaUtils;


public class SendTester
{	
	private static final String TOPIC = "TEST_TOPIC";
	private static final String IP = "239.9.9.10";
	private static final int PORT = 9002;
	private static final String MULTICASTGROUP = PandaUtils.getMulticastGroup(IP, PORT);

	long messageSeqNum;
	long highestSeqNum;
	int returnedMessageCount;
	long startedRecvTimeStamp;
	long endRecvTimeStamp;

	long minRTLatency = Long.MAX_VALUE;
	long maxRTLatency = Long.MIN_VALUE;
	long latencyAccumulator = 0;

	// 24 <= dataSize <= Utils.MAX_MESSAGE_PAYLOAD_SIZE
	public void sendSequencedMessages(int cacheSize, int dataSize, final long numOfMessages, int netRecvBufferSize, int numOfSenderThreads, final String localIp) throws Exception
	{
		final PandaAdapter adapter = new PandaAdapter(cacheSize);

		adapter.subscribe(TOPIC, IP, PORT, MULTICASTGROUP, localIp, new PandaDataListener() {
			private long recdThisSecondTimeStamp;
			private long currentTime;
			private int messagesTillLastSec;

			@Override
			public void receivedPandaData(String topic, ByteBuffer payload)
			{
				this.currentTime = System.currentTimeMillis();
				SendTester.this.messageSeqNum = payload.getLong(8);
				long packetSentTimeStamp = payload.getLong(16);
				SendTester.this.minRTLatency = Math.min(SendTester.this.minRTLatency, (this.currentTime - packetSentTimeStamp));
				SendTester.this.maxRTLatency = Math.max(SendTester.this.maxRTLatency, (this.currentTime - packetSentTimeStamp));
				SendTester.this.latencyAccumulator += (this.currentTime - packetSentTimeStamp);

				SendTester.this.highestSeqNum = Math.max(SendTester.this.highestSeqNum, SendTester.this.messageSeqNum);
				++SendTester.this.returnedMessageCount;

				if (SendTester.this.startedRecvTimeStamp == 0)
				{
					SendTester.this.startedRecvTimeStamp = System.currentTimeMillis();
					this.recdThisSecondTimeStamp = SendTester.this.startedRecvTimeStamp;
				}

				if (this.currentTime > this.recdThisSecondTimeStamp + 1000)
				{
					this.recdThisSecondTimeStamp = this.currentTime;
					System.out.println("--- Recd. " + (SendTester.this.returnedMessageCount - this.messagesTillLastSec) + " Messages In The Last Second. Total Messages Recd. " + SendTester.this.returnedMessageCount);
					this.messagesTillLastSec = SendTester.this.returnedMessageCount;
				}

				SendTester.this.endRecvTimeStamp = System.currentTimeMillis();
			}

			@Override
			public void receivedPandaError(PandaErrorCode issueCode, String multicastGroup, String message, Throwable throable)
			{
				System.err.println(issueCode + "|" + multicastGroup + "|" + message);
			}
		}, netRecvBufferSize, false);

		SenderThread[] senderThreads = new SenderThread[numOfSenderThreads];
		long mssgsPerOtherThreads = numOfMessages / numOfSenderThreads;
		long mssgsPerFirstThread = (numOfMessages - (numOfSenderThreads - 1) * mssgsPerOtherThreads);

		senderThreads[0] = new SenderThread(adapter, dataSize, mssgsPerFirstThread, 0, localIp);
		senderThreads[0].start();
		for (int i = 1; i < numOfSenderThreads; ++i)
		{
			senderThreads[i] = new SenderThread(adapter, dataSize, mssgsPerOtherThreads, i, localIp);
			senderThreads[i].start();
		}

		for (int i = 0; i < numOfSenderThreads; ++i)
		{
			senderThreads[i].join(10000);
		}

		Thread.sleep(10000); // Give enough time for the receiving part of sender

		System.out.println("*** Recd. " + SendTester.this.returnedMessageCount + " Messages In " + (this.endRecvTimeStamp - SendTester.this.startedRecvTimeStamp) + " Milliseconds");
		System.out.println("*** Messages Lost = " + (numOfMessages - SendTester.this.returnedMessageCount) + " (" + (100 * (float) (numOfMessages - SendTester.this.returnedMessageCount) / numOfMessages) + " %)");
		System.out.println("*** Min Round-Trip Latency : " + SendTester.this.minRTLatency + " Milliseconds -- Max Round-Trip Latency : " + SendTester.this.maxRTLatency + " Milliseconds -- Average Round-Trip Latency : "
				+ ((float) SendTester.this.latencyAccumulator / SendTester.this.returnedMessageCount) + " Milliseconds\n");

	}

	public static void runSender(PandaAdapter adapter, String localIp, int dataSize, long numOfMessages, int senderThreadNum) throws Exception
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
			adapter.send(TOPIC, IP, PORT, MULTICASTGROUP, localIp, buffer.array());
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
		int packetSize;
		long numOfMssgs;
		int thisThreadNum;
		String localIp;

		public SenderThread(PandaAdapter adapter, int dataSize, long numOfMessages, int senderThreadNum, String localIp)
		{
			this.pandaAdapter = adapter;
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
				SendTester.runSender(this.pandaAdapter, this.localIp, this.packetSize, this.numOfMssgs, this.thisThreadNum);
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
		int adapterCache = Integer.parseInt(args[0]);
		int numOfMessages = Integer.parseInt(args[1]);
		int payloadSize = Integer.parseInt(args[2]);
		int netRecvBufferSize = Integer.parseInt(args[3]);
		int numOfThreads = Integer.parseInt(args[4]);
		if (numOfThreads < 1 || numOfThreads > 5)
		{
			System.out.println("*** Number of threads should be between 1 and 5");
			System.exit(0);
		}

		String localIp = InetAddress.getLocalHost().getHostAddress();

		SendTester st = new SendTester();
		st.sendSequencedMessages(adapterCache, payloadSize, numOfMessages, netRecvBufferSize, numOfThreads, localIp);
		System.exit(0);
	}
}
