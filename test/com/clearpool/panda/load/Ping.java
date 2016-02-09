package com.clearpool.panda.load;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.clearpool.panda.core.PandaAdapter;
import com.clearpool.panda.core.PandaDataListener;
import com.clearpool.panda.core.PandaErrorCode;
import com.clearpool.panda.core.PandaUtils;

public class Ping implements PandaDataListener
{
	private static final int MESSAGE_LENTH = 100;
	private static final int RECV_BUFFER_SIZE = 10000000;

	private static final String TOPIC1 = "TESTSS1";
	private static final String IP = "239.9.9.10";
	private static final int PORT = 9002;
	private static final String MULTICASTGROUP = PandaUtils.getMulticastGroup(IP, PORT);

	private final PandaAdapter adapter;
	private final int numMessages;
	private final int ignoreCount;
	private final AtomicInteger leftToIgnore;
	private final int messagesPerMilli;
	private final int logRate;
	private final int numThreads;
	private final boolean shouldSubscribe;
	private final List<Thread> sendThreads;
	private final int[] threadSequences;

	private long firstUnignoredMessageTime;
	private long lastUnignoredMessageTime;
	private long messagesReceived;
	private long roundTrip;
	private int errors;

	public Ping(int cacheSize, int numMessages, int messagesPerMilli, int ignoreCount, int logRate, int numThreads, boolean shouldSubscribe) throws Exception
	{
		this.adapter = new PandaAdapter(cacheSize);
		this.numMessages = numMessages;
		this.ignoreCount = ignoreCount;
		this.leftToIgnore = new AtomicInteger(ignoreCount);
		this.messagesPerMilli = messagesPerMilli;
		this.logRate = logRate;
		this.numThreads = numThreads;
		this.shouldSubscribe = shouldSubscribe;
		this.sendThreads = new ArrayList<Thread>();
		this.threadSequences = new int[numThreads];
	}

	private void start() throws Exception
	{
		final InetAddress localIp = InetAddress.getLocalHost();
		if (this.shouldSubscribe) this.adapter.subscribe(TOPIC1, IP, PORT, MULTICASTGROUP, localIp, this, RECV_BUFFER_SIZE, false);

		for (int i = 0; i < this.numThreads; i++)
		{
			final int messageCount = (this.numMessages % this.numThreads == 0) ? this.numMessages / this.numThreads : (i == 0) ? (this.numMessages / this.numThreads)
					+ (this.numMessages % this.numThreads) : (this.numMessages / this.numThreads);
			final int messagesPerMil = (this.messagesPerMilli % this.numThreads == 0) ? this.messagesPerMilli / this.numThreads
					: (i == 0) ? (this.messagesPerMilli / this.numThreads) + (this.messagesPerMilli % this.numThreads) : (this.messagesPerMilli / this.numThreads);
			final int index = i;
			Thread thread = new Thread(new Runnable() {

				@SuppressWarnings("synthetic-access")
				@Override
				public void run()
				{

					try
					{
						// Send
						for (int j = 1; j <= messageCount; j++)
						{
							ByteBuffer buffer = ByteBuffer.allocate(MESSAGE_LENTH);
							buffer.putLong(System.currentTimeMillis());
							buffer.putInt(index);
							buffer.putInt(j);
							buffer.rewind();
							Ping.this.adapter.send(TOPIC1, IP, PORT, MULTICASTGROUP, localIp, buffer.array());
							if (j % messagesPerMil == 0) Thread.sleep(1);
						}
					}
					catch (Exception e)
					{
						e.printStackTrace();
					}
				}
			});
			this.sendThreads.add(thread);
		}

		for (int i = 0; i < this.numThreads; i++)
		{
			this.sendThreads.get(i).start();
		}
	}

	private void printStats()
	{
		long messagesLost = this.numMessages - (this.messagesReceived + this.ignoreCount);
		double avgLatency = this.roundTrip / this.messagesReceived;
		double messagesPerSecond = (this.messagesReceived * 1000) / (this.lastUnignoredMessageTime - this.firstUnignoredMessageTime);
		System.out.println("SUMMARY,Lost=" + messagesLost + ",Errors=" + this.errors + ",AvgLatency=" + avgLatency + ",Msgs/s=" + messagesPerSecond);
	}

	@Override
	public void receivedPandaData(String topic, byte[] bytes)
	{
		ByteBuffer payload = ByteBuffer.wrap(bytes);
		long timestamp = payload.getLong();
		int thread = payload.getInt();
		int sequence = payload.getInt();
		int lastThreadSequence = this.threadSequences[thread];
		if (lastThreadSequence + 1 != sequence)
		{
			System.out.println("Thread=" + thread + " expected=" + (lastThreadSequence + 1) + " got=" + sequence);
		}
		this.threadSequences[thread] = sequence;

		if (this.leftToIgnore.decrementAndGet() < 0)
		{
			long now = System.currentTimeMillis();
			if (this.messagesReceived++ == 0) this.firstUnignoredMessageTime = now;
			this.lastUnignoredMessageTime = now;
			long messageRoundTrip = now - timestamp;

			this.roundTrip += (messageRoundTrip);
			if (this.messagesReceived % this.logRate == 0)
				System.out.println("STAT," + new Date() + "," + this.messagesReceived + "," + messageRoundTrip + "," + ((float) (this.roundTrip / this.messagesReceived)));
		}
	}

	@Override
	public void receivedPandaError(PandaErrorCode arg0, String arg1, String arg2, Throwable arg3)
	{
		this.errors++;
		System.out.println("ERROR," + arg0 + "," + arg1 + "," + arg2);
	}

	public static void main(String[] args) throws Exception
	{
		int cacheSize = Integer.parseInt(args[0]);
		int numMessages = Integer.parseInt(args[1]);
		int messagesPerMilli = Integer.parseInt(args[2]);
		int ignoreCount = Integer.parseInt(args[3]);
		int logRate = Integer.parseInt(args[4]);
		int numThreads = Integer.parseInt(args[5]);
		boolean shouldSubscribe = Boolean.parseBoolean(args[6]);
		numMessages += ignoreCount;

		Ping ping = new Ping(cacheSize, numMessages, messagesPerMilli, ignoreCount, logRate, numThreads, shouldSubscribe);
		ping.start();
		while (ping.sendThreads.size() > 0)
		{
			Iterator<Thread> sendThreadIterator = ping.sendThreads.iterator();
			while (sendThreadIterator.hasNext())
			{
				Thread sendThread = sendThreadIterator.next();
				if (!sendThread.isAlive())
				{
					sendThreadIterator.remove();
				}
			}
			Thread.sleep(1000);
		}
		ping.printStats();
		System.exit(0);
	}
}