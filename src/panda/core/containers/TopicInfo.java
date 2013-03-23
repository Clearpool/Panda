package panda.core.containers;

public class TopicInfo
{
	private final String ip;
	private final Integer port;
	private final Short topicId;
	private final String topic;
	private final String multicastGroup;
	
	public TopicInfo(String ip, Integer port, Short topicId, String topic)
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

	public Short getTopicId()
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