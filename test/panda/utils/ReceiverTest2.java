package panda.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Date;

import panda.core.PandaAdapter;
import panda.core.PandaDataListener;
import panda.core.PandaErrorCode;
import panda.core.PandaTopicInfo;


public class ReceiverTest2
{
	public static void main(String[] args) throws Exception
	{
		final PandaAdapter adapter = new PandaAdapter(0);
		final PandaTopicInfo topicInfo2 = new PandaTopicInfo("239.9.9.10", Integer.valueOf(9002), Integer.valueOf((short)2), "FOUR");
		adapter.subscribe(topicInfo2, getLocalIp(null), new PandaDataListener() {
			@Override
			public void receivedPandaData(int topicId, ByteBuffer payload)
			{
				byte[] bytes = new byte[payload.remaining()];
				payload.get(bytes, 0, bytes.length);
				System.out.println(new Date() + " Received packet=" + new String(bytes));
			}

			@Override
			public void receivedPandaError(PandaErrorCode issueCode, String multicastGroup, String message, Throwable throable)
			{
				System.err.println(issueCode + "|" + multicastGroup + "|" + message);
			}
		}, 10000000);
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