package com.clearpool.panda.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.clearpool.panda.core.PandaAdapter;
import com.clearpool.panda.core.PandaUtils;

public class SendTest
{
	private static final String TOPIC = "ONE";
	private static final String IP = "239.9.9.10";
	private static final int PORT = 9002;
	static final String MULTICASTGROUP = PandaUtils.getMulticastGroup(IP, PORT);
	static final String LOCAL_IP = getLocalIp(null);

	public static void main(String[] args) throws Exception
	{
		final PandaAdapter adapter = new PandaAdapter(1000);
		for (int n = 0; n < 5; n++)
		{
			long start = System.currentTimeMillis();
			SendTester s1 = new SendTester(adapter);
			s1.start();
			s1.join();
			System.out.println("Test took " + (System.currentTimeMillis() - start) + " millis");
			Thread.sleep(2000);
		}
		System.exit(0);
	}

	static class SendTester extends Thread
	{
		private final PandaAdapter adapter;

		public SendTester(PandaAdapter adapter)
		{
			this.adapter = adapter;
		}

		@Override
		public void run()
		{
			for (int i = 0; i < 1000000; i++)
			{
				try
				{
					this.adapter.send(TOPIC, IP, PORT, MULTICASTGROUP, LOCAL_IP, String.valueOf(i).getBytes());
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
			}
		}
	}

	private static String getLocalIp(String ip)
	{
		if (ip == null)
		{
			try
			{
				return InetAddress.getLocalHost().getHostAddress();
			}
			catch (UnknownHostException e)
			{
				return null;
			}
		}
		return ip;
	}
}