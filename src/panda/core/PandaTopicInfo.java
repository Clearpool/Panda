package panda.core;

public class PandaTopicInfo
{
	private final String ip;
	private final Integer port;
	private final Integer topicId;
	private final String topic;
	private final String multicastGroup;
	
	public PandaTopicInfo(String ip, Integer port, Integer topicId, String topic)
	{
		this.ip = ip;
		this.port = port;
		this.topicId = topicId;
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

	public Integer getTopicId()
	{
		return this.topicId;
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