package panda.core;

public class TestChannelReceiveInfo extends ChannelReceiveInfo
{
	public TestChannelReceiveInfo(String multicastIp, int multicastPort, String multicastGroup, String localIp, int bindPort, SelectorThread selectorThread, int recvBufferSize)
	{
		super(multicastIp, multicastPort, multicastGroup, localIp, bindPort, selectorThread, recvBufferSize);
	}
}
