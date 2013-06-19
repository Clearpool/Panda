package panda.utils;

import java.net.InetAddress;

import panda.core.PandaAdapter;
import panda.core.PandaTopicInfo;

public class PandaSend
{
	public static void main(String[] args) throws Exception
	{
		if (args.length < 5)
		{
			System.out.println("Min. 4 args required <ip,port,topicString,DATA>");
		}
		else
		{
			String ip = args[0];
			int port = Integer.parseInt(args[1]);
			String topic = args[2];
			String interfaceIp = InetAddress.getLocalHost().getHostAddress();
			String data = args[3];

			PandaAdapter adapter = new PandaAdapter(0);
			PandaTopicInfo topicInfo = new PandaTopicInfo(ip, Integer.valueOf(port), topic);

			adapter.send(topicInfo, interfaceIp, data.getBytes());
			Thread.sleep(10);
			System.exit(0);
		}
	}
}