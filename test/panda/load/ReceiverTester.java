package panda.load;

import java.net.InetAddress;
import java.nio.ByteBuffer;

import panda.core.PandaAdapter;
import panda.core.PandaDataListener;
import panda.core.PandaErrorCode;
import panda.core.PandaTopicInfo;

public class ReceiverTester
{
	private long highestSeqNumber = 0;
	private long latestMessageCount = 0;

	public void setHighestSeqNum(long l)
	{
		this.highestSeqNumber = l;
	}

	public long getHighestSeqNum()
	{
		return this.highestSeqNumber;
	}

	public void setLatestMessageCount(long l)
	{
		this.latestMessageCount = l;
	}

	public long getLatestMessageCount()
	{
		return this.latestMessageCount;
	}

	public void subscribeToSequencedMessages(int cacheSize, final PandaTopicInfo topicInfo, final long numOfMessages, int netRecvBufferSize, final String localIp) throws Exception
	{
		final PandaAdapter adapter = new PandaAdapter(cacheSize);
		final ReceiverWatchdog wd = new ReceiverWatchdog(3000, 250, this, numOfMessages);

		new Thread(wd).start();

		adapter.subscribe(topicInfo, localIp, new PandaDataListener() {
			private long messageSeqNum = 0;
			private long highestRecdSeqNum = 0;
			private long messageCount = 0;
			private long messagesTillLastSec = 0;

			private long startedRecvTimeStamp = 0;
			private long recdThisSecondTimeStamp = 0;
			private long currentTime = 0;
			private long endRecvTimeStamp;

			@Override
			public void receivedPandaData(String topic, ByteBuffer payload)
			{
				wd.restart();
				this.messageSeqNum = payload.getLong(8);
				this.highestRecdSeqNum = Math.max(this.highestRecdSeqNum, this.messageSeqNum);
				++this.messageCount;
				ReceiverTester.this.setHighestSeqNum(this.messageSeqNum);
				ReceiverTester.this.setLatestMessageCount(this.messageCount);

				if (this.startedRecvTimeStamp == 0)
				{
					this.startedRecvTimeStamp = System.currentTimeMillis();
					this.recdThisSecondTimeStamp = this.startedRecvTimeStamp;
				}

				if ((this.currentTime = System.currentTimeMillis()) > this.recdThisSecondTimeStamp + 1000)
				{
					this.recdThisSecondTimeStamp = this.currentTime;
					if (this.messageCount <= numOfMessages) System.out.println("--- Recd. " + (this.messageCount - this.messagesTillLastSec) + " Messages In The Last Second. Total Messages Recd. " + this.messageCount);
					this.messagesTillLastSec = this.messageCount;
				}

				if (this.messageCount == numOfMessages)
				{
					this.endRecvTimeStamp = System.currentTimeMillis();
					System.out.println("*** Recd. " + this.messageCount + " In " + (this.endRecvTimeStamp - this.startedRecvTimeStamp) + " Milliseconds");
				}

				try
				{
					payload.position(0);
					adapter.send(topicInfo, localIp, payload.array());
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
			}

			@Override
			public void receivedPandaError(PandaErrorCode issueCode, String multicastGroup, String message, Throwable throable)
			{
				System.err.println(issueCode + "|" + multicastGroup + "|" + message);
			}
		}, netRecvBufferSize);
	}
}

class ReceiverWatchdog implements Runnable
{
	final long resetTimeMillis;
	final int sleepResolution;
	final ReceiverTester recvTester;
	final long numOfMssgs;

	long timeStamp;

	public ReceiverWatchdog(int resetTimeMillis, int resolutionMillis, ReceiverTester rt, long numOfMessages)
	{
		this.resetTimeMillis = resetTimeMillis;
		this.sleepResolution = resolutionMillis;
		this.recvTester = rt;
		this.numOfMssgs = numOfMessages;
	}

	public void restart()
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
				Thread.sleep(this.sleepResolution);
			}
			catch (InterruptedException e)
			{
				e.printStackTrace();
			}
			if (System.currentTimeMillis() > (this.timeStamp + this.resetTimeMillis))
			{
				System.out.println("*** Recd. " + this.recvTester.getLatestMessageCount() + " Messages From Sender"/* . Last Recd. Sender Seq. # " + this.recvTester.getHighestSeqNum() */);
				System.out.println("*** Messages Lost " + (this.numOfMssgs - this.recvTester.getLatestMessageCount()) + " (" + (100 * (float) (this.numOfMssgs - this.recvTester.getLatestMessageCount()) / this.numOfMssgs) + " %)");
				System.exit(0);
			}
		}
	}

	public static void main(String[] args) throws Exception
	{
		if (args.length != 4)
		{
			System.out.println(" usage -- panda.core.RecvTest2 (int)pandaAdapterCache (long)numOfMessages (int)netRecvBufferSize");
			System.exit(0);
		}

		int adapterCache = Integer.valueOf(args[0]).intValue();
		long numOfMessages = Long.valueOf(args[1]).longValue();
		int netRecvBufferSize = Integer.valueOf(args[2]).intValue();
		PandaTopicInfo topicInfo = new PandaTopicInfo("239.9.9.10", Integer.valueOf(9002), "TEST_TOPIC");
		String localIp = InetAddress.getLocalHost().getHostAddress();

		ReceiverTester rt = new ReceiverTester();
		rt.subscribeToSequencedMessages(adapterCache, topicInfo, numOfMessages, netRecvBufferSize, localIp);
	}
}