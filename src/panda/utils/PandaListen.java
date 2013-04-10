package panda.utils;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;

import panda.core.PandaAdapter;
import panda.core.PandaDataListener;
import panda.core.PandaErrorCode;
import panda.core.PandaTopicInfo;


public class PandaListen implements PandaDataListener
{
	private final PandaAdapter adapter;
	private final PandaTopicInfo topicInfo;
	
	public PandaListen(String ip, int port, int topicId, String topic) throws Exception
	{
		this.adapter = new PandaAdapter(0);
		this.topicInfo = new PandaTopicInfo(ip, Integer.valueOf(port), Integer.valueOf(topicId), topic);
	}
	
	public void start(String interfaceIp)
	{
		this.adapter.subscribe(this.topicInfo, interfaceIp, this, 100000);
	}
	
	@Override
	public void receivedPandaData(int topicId, ByteBuffer payload)
	{
		StringBuilder builder = new StringBuilder();
		builder.append(new Date());
		builder.append("|topicID=");
		builder.append(topicId);
		builder.append("|length=");
		builder.append(payload.remaining());
		System.out.println(builder.toString());
	}

	@Override
	public void receivedPandaError(PandaErrorCode issueCode, String multicastGroup, String message, Throwable throwable)
	{
		System.out.println(issueCode + "|" + multicastGroup + "|" + message);
	}
	
	public static void main(String[] args) throws Exception
	{
		if(args.length < 4)
		{
			System.out.println("Min. 4 args required <topicIp,topicPort,topicId,topicString>");
		}
		else
		{
			String ip = args[0];
			int port = Integer.parseInt(args[1]);
			int topicId = Integer.parseInt(args[2]);
			String topic = args[3];
			String interfaceIp = InetAddress.getLocalHost().getHostAddress();
			
			PandaListen pandaListener = new PandaListen(ip, port, topicId, topic);
			pandaListener.start(interfaceIp);
		}
	}
}
