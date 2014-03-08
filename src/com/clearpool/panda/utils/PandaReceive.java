package com.clearpool.panda.utils;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;

import com.clearpool.panda.core.PandaAdapter;
import com.clearpool.panda.core.PandaDataListener;
import com.clearpool.panda.core.PandaErrorCode;
import com.clearpool.panda.core.PandaUtils;



public class PandaReceive implements PandaDataListener
{
	private final PandaAdapter adapter;
	
	public PandaReceive() throws Exception
	{
		this.adapter = new PandaAdapter(0);
	}
	
	public void start(String topic, String ip, int port, String multicastGroup, String interfaceIp)
	{
		this.adapter.subscribe(topic, ip, port, multicastGroup, interfaceIp, this, 100000);
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
		if(args.length < 3)
		{
			System.out.println("Min. 3 args required <ip,port,topicString>");
		}
		else
		{
			String ip = args[0];
			int port = Integer.parseInt(args[1]);
			String topic = args[2];
			String interfaceIp = InetAddress.getLocalHost().getHostAddress();
			
			PandaReceive pandaListener = new PandaReceive();
			pandaListener.start(topic, ip, port, PandaUtils.getMulticastGroup(ip, port), interfaceIp);
		}
	}
}
