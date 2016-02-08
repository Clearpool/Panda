package com.clearpool.panda.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.clearpool.panda.core.PandaAdapter;

public class SendTest
{
	private static final String TOPIC = "ONE";
	private static final String IP = "239.9.9.10";
	private static final int PORT = 9002;
	private static final String MULTICASTGROUP = IP + ":" + PORT;
	private static final InetAddress LOCAL_IP = getLocalIp();

	public static void main(String[] args) throws Exception
	{
		final PandaAdapter adapter = new PandaAdapter(1000);
		int count = 0;
		long start = System.currentTimeMillis();

		while (true)
		{
			count++;
			String countString = String.valueOf(count);
			adapter.send(TOPIC, IP, PORT, MULTICASTGROUP, LOCAL_IP, countString.getBytes());
			if (count % 100000 == 0)
			{
				long end = System.currentTimeMillis();
				System.out.println("Took " + (end - start) + " milliseconds to send 100000 messages to multicast");
				start = end;
			}
			if (count % 500 == 0) Thread.sleep(1);
		}
	}

	private static InetAddress getLocalIp()
	{
		try
		{
			return InetAddress.getLocalHost();
		}
		catch (UnknownHostException e)
		{
			return null;
		}
	}
}