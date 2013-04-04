package panda.tester;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import panda.core.PandaAdapter;

import panda.core.IDataListener;
import panda.core.containers.TopicInfo;

public class ReceiverTester
{
	public void SubscribeToSequencedMessages(int cacheSize, TopicInfo tInfo, final long numOfMessages, int netRecvBufferSize) throws IOException
	{
		final PandaAdapter adapter = new PandaAdapter(cacheSize);
		final TopicInfo topicInfo = tInfo;

		final Watchdog wd = new Watchdog(5000, 500);
		new Thread(wd).start();

		adapter.subscribe(topicInfo, InetAddress.getLocalHost().getHostAddress(), new IDataListener() {
			private long senderStartTimeStamp = 0;
			private long messageSeqNum = 0;
			private long messageCount = 0;
			private long messagesTillLastSec = 0;

			private long startedRecvTimeStamp = 0;
			private long recdThisSecondTimeStamp = 0;
			private long currentTime = 0;
			private long endRecvTimeStamp;

			@Override
			public void receivedPandaData(int topicId, ByteBuffer payload)
			{
				wd.Restart();
				this.senderStartTimeStamp = payload.getLong();
				this.messageSeqNum = payload.getLong();
				++this.messageCount;

				if (this.startedRecvTimeStamp == 0)
				{
					this.startedRecvTimeStamp = System.currentTimeMillis();
					this.recdThisSecondTimeStamp = this.startedRecvTimeStamp;
				}

				if ((this.currentTime = System.currentTimeMillis()) > this.recdThisSecondTimeStamp + 1000)
				{
					this.recdThisSecondTimeStamp = this.currentTime;
					System.out.println("Recd. " + (this.messageCount - this.messagesTillLastSec) + " messages in the last second. Total Messages Recd. " + this.messageCount);
					this.messagesTillLastSec = this.messageCount;
				}

				/*if (this.messageSeqNum == numOfMessages)
				{
					this.endRecvTimeStamp = System.currentTimeMillis();
					System.out.println("Recd. " + this.messageCount + " in " + (this.endRecvTimeStamp - this.startedRecvTimeStamp) + " milliseconds");
				}*/
				
				try
				{
					payload.position(0);
					adapter.send(topicInfo, InetAddress.getLocalHost().getHostAddress(), payload.array());
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
				
			}
		}, netRecvBufferSize);
	}

}

class Watchdog implements Runnable
{
	final long resetTimeMillis;
	long timeStamp;
	final int sleepResolution;

	public Watchdog(int resetTimeMillis, int resolutionMillis)
	{
		this.resetTimeMillis = resetTimeMillis;
		this.sleepResolution = resolutionMillis;
	}

	public void Restart()
	{
		this.timeStamp = System.currentTimeMillis();
	}

	@Override
	public void run()
	{
		this.timeStamp = System.currentTimeMillis();

		while (true)
		{
			try
			{
				Thread.sleep(500);
			}
			catch (InterruptedException e)
			{
				e.printStackTrace();
			}
			if (System.currentTimeMillis() > (this.timeStamp + this.resetTimeMillis))
			{
				//System.out.println("Watchdog Killing Process");
				System.exit(0);
			}
		}

	}

}
