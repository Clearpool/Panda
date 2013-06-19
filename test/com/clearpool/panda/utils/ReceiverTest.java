package com.clearpool.panda.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Date;

import com.clearpool.panda.core.PandaAdapter;
import com.clearpool.panda.core.PandaDataListener;
import com.clearpool.panda.core.PandaErrorCode;
import com.clearpool.panda.core.PandaUtils;



public class ReceiverTest
{
	private static final String TOPIC1 = "ONE";
	private static final String TOPIC2 = "TWO";
	private static final String IP = "239.9.9.10";
	private static final int PORT = 9002;
	private static final String MULTICASTGROUP = PandaUtils.getMulticastGroup(IP, PORT);
	
	public static void main(String[] args) throws Exception
	{
		final PandaAdapter adapter = new PandaAdapter(0);
		
		adapter.subscribe(TOPIC1, IP, PORT, MULTICASTGROUP, getLocalIp(null), new PandaDataListener() {
			@Override
			public void receivedPandaData(String topic, ByteBuffer payload)
			{
				byte[] bytes = new byte[payload.remaining()];
				payload.get(bytes, 0, bytes.length);
				String string = new String(bytes);
				int stringValue = Integer.parseInt(string);
				if (stringValue % 100000 == 0) System.out.println(new Date() + " Received packet=" + string);
			}

			@Override
			public void receivedPandaError(PandaErrorCode issueCode, String multicastGroup, String message, Throwable throable)
			{
				System.err.println(issueCode + "|" + multicastGroup + "|" + message);
			}
		}, 10000000);
		adapter.subscribe(TOPIC2, IP, PORT, MULTICASTGROUP, getLocalIp(null), new PandaDataListener() {
			@Override
			public void receivedPandaData(String topic, ByteBuffer payload)
			{
				byte[] bytes = new byte[payload.remaining()];
				payload.get(bytes, 0, bytes.length);
				String string = new String(bytes);
				int stringValue = Integer.parseInt(string);
				if (stringValue % 100000 == 0) System.out.println(new Date() + " Received packet=" + string);
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