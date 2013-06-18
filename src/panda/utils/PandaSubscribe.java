package panda.utils;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;

import panda.core.PandaAdapter;
import panda.core.PandaDataListener;
import panda.core.PandaErrorCode;
import panda.core.PandaTopicInfo;


public class PandaSubscribe implements PandaDataListener
{
	private final PandaAdapter adapter;
	private final PandaTopicInfo topicInfo;
	
	public PandaSubscribe(String ip, int port, String topic) throws Exception
	{
		this.adapter = new PandaAdapter(0);
		this.topicInfo = new PandaTopicInfo(ip, Integer.valueOf(port), topic);
	}
	
	public void start(String interfaceIp)
	{
		this.adapter.subscribe(this.topicInfo, interfaceIp, this, 100000);
	}
	
	@Override
	public void receivedPandaData(String topic, ByteBuffer payload)
	{
		StringBuilder builder = new StringBuilder();
		builder.append(new Date());
		builder.append("|topic=");
		builder.append(topic);
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
			System.out.println("Min. 3 args required <ip,port,topicString>");
		}
		else
		{
			String ip = args[0];
			int port = Integer.parseInt(args[1]);
			String topic = args[2];
			String interfaceIp = InetAddress.getLocalHost().getHostAddress();
			
			PandaSubscribe pandaListener = new PandaSubscribe(ip, port, topic);
			pandaListener.start(interfaceIp);
		}
	}
}
