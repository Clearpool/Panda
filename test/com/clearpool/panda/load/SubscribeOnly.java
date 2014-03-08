package com.clearpool.panda.load;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.clearpool.panda.core.PandaAdapter;
import com.clearpool.panda.core.PandaDataListener;
import com.clearpool.panda.core.PandaErrorCode;
import com.clearpool.panda.core.PandaUtils;

public class SubscribeOnly implements PandaDataListener
{
	private static final String TOPIC1 = "TESTSS1";
	private static final String IP = "239.9.9.10";
	private static final int PORT = 9002;
	private static final String MULTICASTGROUP = PandaUtils.getMulticastGroup(IP, PORT);
	private static final int RECV_BUFFER_SIZE = 10000000;

	private final PandaAdapter adapter;
	private final String localIp;

	private int messagesReceived;
	private long timeLastReceived;
	private int errors;

	public SubscribeOnly(int cacheSize) throws Exception
	{
		this.adapter = new PandaAdapter(cacheSize);
		this.localIp = InetAddress.getLocalHost().getHostAddress();
	}

	private void start() throws Exception
	{
		this.adapter.subscribe(TOPIC1, IP, PORT, MULTICASTGROUP, this.localIp, this, RECV_BUFFER_SIZE);
	}

	private void printStats()
	{
		System.out.println("LastMsgReceived-" + new SimpleDateFormat("HH:mm:ss:SSS").format(new Date(this.timeLastReceived)));
		System.out.println("SUMMARY,MessagesReceived=" + this.messagesReceived + ",Errors=" + this.errors);
	}

	@Override
	public void receivedPandaData(String topic, ByteBuffer arg1)
	{
		try
		{
			if (topic.equals(TOPIC1))
			{
				this.messagesReceived++;
				this.timeLastReceived = System.currentTimeMillis();
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

		SubscribeOnly subscriber = new SubscribeOnly(cacheSize);
		subscriber.start();
		while (subscriber.messagesReceived == 0 || subscriber.messagesReceived > 0 && (System.currentTimeMillis() - subscriber.timeLastReceived < 1000))
		{
			Thread.sleep(1000);
		}
		System.out.println(new SimpleDateFormat("HH:mm:ss:SSS").format(new Date()));
		subscriber.printStats();
		System.exit(0);
	}
}
