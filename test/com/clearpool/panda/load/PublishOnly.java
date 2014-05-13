package com.clearpool.panda.load;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import com.clearpool.panda.core.PandaAdapter;
import com.clearpool.panda.core.PandaUtils;

public class PublishOnly
{
	private static final int MESSAGE_LENTH = 100;

	private static final String TOPIC1 = "TESTSS1";
	private static final String IP = "239.9.9.10";
	private static final int PORT = 9002;
	private static final String MULTICASTGROUP = PandaUtils.getMulticastGroup(IP, PORT);

	private final PandaAdapter adapter;
	private final int numMessages;
	private final int messagesPerMilli;
	private final int numThreads;
	private final List<Thread> sendThreads;

	public PublishOnly(int cacheSize, int numMessages, int messagesPerMilli, int numThreads) throws Exception
	{
		this.adapter = new PandaAdapter(cacheSize);
		this.numMessages = numMessages;
		this.messagesPerMilli = messagesPerMilli;
		this.numThreads = numThreads;
		this.sendThreads = new ArrayList<Thread>();
	}

	private void start() throws Exception
	{
		final String localIp = InetAddress.getLocalHost().getHostAddress();

		for (int i = 0; i < this.numThreads; i++)
		{
			final int messageCount = (this.numMessages % this.numThreads == 0) ? this.numMessages / this.numThreads : (i == 0) ? (this.numMessages / this.numThreads) + (this.numMessages % this.numThreads)
					: (this.numMessages / this.numThreads);
			final int messagesPerMil = (this.messagesPerMilli % this.numThreads == 0) ? this.messagesPerMilli / this.numThreads : (i == 0) ? (this.messagesPerMilli / this.numThreads) + (this.messagesPerMilli % this.numThreads)
					: (this.messagesPerMilli / this.numThreads);
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
							PublishOnly.this.adapter.send(TOPIC1, IP, PORT, MULTICASTGROUP, localIp, buffer.array());
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

	public static void main(String[] args) throws Exception
	{
		int cacheSize = Integer.parseInt(args[0]);
		int numMessages = Integer.parseInt(args[1]);
		int messagesPerMilli = Integer.parseInt(args[2]);
		int numThreads = Integer.parseInt(args[3]);

		PublishOnly publisher = new PublishOnly(cacheSize, numMessages, messagesPerMilli, numThreads);
		long start = System.currentTimeMillis();
		System.out.println("Start-" + new SimpleDateFormat("HH:mm:ss:SSS").format(new Date()));
		publisher.start();
		while (publisher.sendThreads.size() > 0)
		{
			Iterator<Thread> sendThreadIterator = publisher.sendThreads.iterator();
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
		long end = System.currentTimeMillis();
		System.out.println("Sent " + numMessages + " on " + numThreads + " threads in " + (end - start) + "ms");
		System.exit(0);
	}
}
