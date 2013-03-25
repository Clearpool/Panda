package panda.core;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;

import panda.core.Adapter;
import panda.core.containers.TopicInfo;


public class SenderTest
{
	public static void main(String[] args) throws Exception
	{
		final Adapter adapter = new Adapter(1000);
		final TopicInfo topicInfo1 = new TopicInfo("239.9.9.9", Integer.valueOf(9001), Short.valueOf((short)1), "FIVE");

		int count = 0;
		while (true)
		{
			count++;
			String countString = String.valueOf(count);
			adapter.send(topicInfo1, getLocalIp(null), countString.getBytes());
			System.out.println(new Date() + " Sent packet=" + countString);
			Thread.sleep(1000L);
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