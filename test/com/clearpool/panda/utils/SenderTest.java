package com.clearpool.panda.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;

import com.clearpool.panda.core.PandaAdapter;
import com.clearpool.panda.core.PandaUtils;


public class SenderTest
{
	private static final String TOPIC1 = "ONE";
	private static final String IP = "239.9.9.10";
	private static final int PORT = 9002;
	private static final String MULTICASTGROUP = PandaUtils.getMulticastGroup(IP, PORT);
	
	public static void main(String[] args) throws Exception
	{
		final PandaAdapter adapter = new PandaAdapter(1000);
		final String localIp = getLocalIp(null);

		int count = 0;
		while (true)
		{
			for (int i = 0; i < 500; i++)
			{
				count++;
				String countString = String.valueOf(count);
				adapter.send(TOPIC1, IP, PORT, MULTICASTGROUP, localIp, countString.getBytes());
				if (count % 100000 == 0)
				{
					System.out.println(new Date());
				}
			}
			Thread.sleep(1);
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