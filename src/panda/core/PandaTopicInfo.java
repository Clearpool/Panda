package panda.core;

public class PandaTopicInfo
{
	private final String ip;
	private final Integer port;
	private final String topic;
	private final String multicastGroup;
	
	public PandaTopicInfo(String ip, Integer port, String topic)
	{
		this.ip = ip;
		this.port = port;
		this.topic = topic;
		this.multicastGroup = ip + ":" + port;
	}

	public String getIp()
	{
		return this.ip;
	}

	public Integer getPort()
	{
		return this.port;
	}

	public String getTopic()
	{
		return this.topic;
	}
	
	public String getMulticastGroup()
	{
		return this.multicastGroup;
	}
}