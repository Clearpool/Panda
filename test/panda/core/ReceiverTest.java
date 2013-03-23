package panda.core;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Date;

import panda.core.Adapter;
import panda.core.IDataListener;
import panda.core.containers.TopicInfo;


public class ReceiverTest
{
	public static void main(String[] args) throws IOException
	{
		final Adapter adapter = new Adapter(0);
		final TopicInfo topicInfo1 = new TopicInfo("239.9.9.9", Integer.valueOf(9001), Short.valueOf((short)1), "FIVE");
		final TopicInfo topicInfo2 = new TopicInfo("239.9.9.9", Integer.valueOf(9001), Short.valueOf((short)2), "FOUR");
		
		adapter.subscribe(topicInfo1, getLocalIp(null), new IDataListener() {
			@Override
			public void receivedFmcData(ByteBuffer payload)
			{
				byte[] bytes = new byte[payload.remaining()];
				payload.get(bytes, 0, bytes.length);
				String string = new String(bytes);
				int stringValue = Integer.parseInt(string);
				if (stringValue % 1 == 0) System.out.println(new Date() + " Received packet=" + string);
			}
		});
		adapter.subscribe(topicInfo2, getLocalIp(null), new IDataListener() {
			@Override
			public void receivedFmcData(ByteBuffer payload)
			{
				byte[] bytes = new byte[payload.remaining()];
				payload.get(bytes, 0, bytes.length);
				String string = new String(bytes);
				int stringValue = Integer.parseInt(string);
				if (stringValue % 1 == 0) System.out.println(new Date() + " Received packet=" + string);
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