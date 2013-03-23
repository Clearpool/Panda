package panda.core;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Date;

import panda.core.Adapter;
import panda.core.IDataListener;
import panda.core.containers.TopicInfo;


public class ReceiverTest2
{
	public static void main(String[] args) throws IOException
	{
		final Adapter adapter = new Adapter(0);
		final TopicInfo topicInfo2 = new TopicInfo("239.9.9.9", Integer.valueOf(9001), Short.valueOf((short)2), "FOUR");
		adapter.subscribe(topicInfo2, getLocalIp(null), new IDataListener() {
			@Override
			public void receivedFmcData(ByteBuffer payload)
			{
				byte[] bytes = new byte[payload.remaining()];
				payload.get(bytes, 0, bytes.length);
				System.out.println(new Date() + " Received packet=" + new String(bytes));
			}
		});
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