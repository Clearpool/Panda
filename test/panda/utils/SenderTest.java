package panda.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;

import panda.core.PandaAdapter;
import panda.core.PandaTopicInfo;


public class SenderTest
{
	public static void main(String[] args) throws Exception
	{
		final PandaAdapter adapter = new PandaAdapter(1000);
		final PandaTopicInfo topicInfo1 = new PandaTopicInfo("239.9.9.10", Integer.valueOf(9002), Integer.valueOf((short)1), "FIVE");
		final String localIp = getLocalIp(null);
		
		int count = 0;
		while (true)
		{
			for(int i=0; i<500; i++)
			{
				count++;
				String countString = String.valueOf(count);
				adapter.send(topicInfo1, localIp, countString.getBytes());
				//System.out.println(new Date() + " Sent packet=" + countString);
				//Thread.sleep(1000L);
				if(count % 100000 == 0)
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