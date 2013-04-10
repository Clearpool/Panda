package panda.utils;

import java.net.InetAddress;

import panda.core.PandaAdapter;
import panda.core.PandaTopicInfo;

public class PandaSend
{
	public static void main(String[] args) throws Exception
	{
		if(args.length < 5)
		{
			System.out.println("Min. 5 args required <topicIp,topicPort,topicId,topicString,DATA>");
		}
		else
		{
			String ip = args[0];
			int port = Integer.parseInt(args[1]);
			int topicId = Integer.parseInt(args[2]);
			String topic = args[3];
			String interfaceIp = InetAddress.getLocalHost().getHostAddress();
			String data = args[4];
			
			PandaAdapter adapter = new PandaAdapter(0);
			PandaTopicInfo topicInfo = new PandaTopicInfo(ip, Integer.valueOf(port), Integer.valueOf(topicId), topic);
			
			adapter.send(topicInfo, interfaceIp, data.getBytes());
			Thread.sleep(10);
			System.exit(0);
		}
	}
}
