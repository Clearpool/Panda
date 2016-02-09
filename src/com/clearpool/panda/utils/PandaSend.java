package com.clearpool.panda.utils;

import java.net.InetAddress;

import com.clearpool.panda.core.PandaAdapter;
import com.clearpool.panda.core.PandaUtils;

public class PandaSend
{
	public static void main(String[] args) throws Exception
	{
		if (args.length < 4)
		{
			System.out.println("Min. 4 args required <ip,port,topicString,DATA>");
		}
		else
		{
			String ip = args[0];
			int port = Integer.parseInt(args[1]);
			String topic = args[2];
			InetAddress interfaceIp = InetAddress.getLocalHost();
			String data = args[3];

			PandaAdapter adapter = new PandaAdapter(0);

			adapter.send(topic, ip, port, PandaUtils.getMulticastGroup(ip, port), interfaceIp, data.getBytes());
			Thread.sleep(10);
			System.exit(0);
		}
	}
}