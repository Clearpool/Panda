package com.clearpool.panda.load;

import java.net.InetAddress;
import java.nio.ByteBuffer;

import com.clearpool.panda.core.PandaAdapter;
import com.clearpool.panda.core.PandaDataListener;
import com.clearpool.panda.core.PandaErrorCode;
import com.clearpool.panda.core.PandaTopicInfo;


public class Pong implements PandaDataListener
{
	private static final PandaTopicInfo TOPICINFO1 = new PandaTopicInfo("239.9.9.10", Integer.valueOf(9002), "TESTSS1");
	private static final PandaTopicInfo TOPICINFO2 = new PandaTopicInfo("239.9.9.10", Integer.valueOf(9002), "TESTSS2");
	private static final int RECV_BUFFER_SIZE = 10000000;

	private final PandaAdapter adapter;
	private final String localIp;
	private final boolean shouldPong;

	private int messagesReceived;
	private long timeLastReceived;
	private int errors;

	public Pong(int cacheSize, boolean shouldPong) throws Exception
	{
		this.adapter = new PandaAdapter(cacheSize);
		this.localIp = InetAddress.getLocalHost().getHostAddress();
		this.shouldPong = shouldPong;
	}

	private void start() throws Exception
	{
		this.adapter.subscribe(TOPICINFO1, this.localIp, this, RECV_BUFFER_SIZE);
		this.adapter.subscribe(TOPICINFO2, this.localIp, this, RECV_BUFFER_SIZE);
	}

	private void printStats()
	{
		System.out.println("SUMMARY,MessagesReceived=" + this.messagesReceived + ",Errors=" + this.errors);
	}

	@Override
	public void receivedPandaData(String topic, ByteBuffer arg1)
	{
		try
		{
			if (topic.equals("TESTSS1"))
			{
				this.messagesReceived++;
				this.timeLastReceived = System.currentTimeMillis();
				if (this.shouldPong) this.adapter.send(TOPICINFO1, this.localIp, arg1.array());
			}
			else if (topic.equals("TESTSS2"))
			{
				this.messagesReceived++;
				this.timeLastReceived = System.currentTimeMillis();
				if (this.shouldPong) this.adapter.send(TOPICINFO2, this.localIp, arg1.array());
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
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
		int cacheSize = Integer.valueOf(args[0]).intValue();
		boolean shouldPong = Boolean.valueOf(args[1]).booleanValue();

		Pong pong = new Pong(cacheSize, shouldPong);
		pong.start();
		while (pong.messagesReceived == 0 || pong.messagesReceived > 0 && (System.currentTimeMillis() - pong.timeLastReceived < 1000))
		{
			Thread.sleep(1000);
		}
		pong.printStats();
		System.exit(0);
	}
}