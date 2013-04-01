package panda.core;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

import panda.core.containers.TopicInfo;

//Do not subscribe to same group on two diff network cards in same adapter
//TODO - TEST - can multiple adapters co-exist in same process space?
//TODO - TEST - unit tests
public class Adapter
{
	private static final Logger LOGGER = Logger.getLogger(Adapter.class.getName());

	private final SelectorThread selectorThread;
	private final Receiver receiver;
	private final Sender sender;

	public Adapter(int cacheSize) throws IOException
	{
		this.selectorThread = new SelectorThread();
		ServerSocketChannel channel = getServerSocketChannel();
		this.receiver = new Receiver(this.selectorThread, channel.socket().getLocalPort());
		this.sender = new Sender(this.selectorThread, channel, cacheSize);
		this.selectorThread.createOutDatagramChannel(channel.socket().getLocalPort());
		this.selectorThread.start();
	}

	private static ServerSocketChannel getServerSocketChannel()
	{
		try
		{
			ServerSocketChannel channel = ServerSocketChannel.open();
			channel.configureBlocking(false);
			channel.bind(new InetSocketAddress(0));
			return channel;
		} catch (Exception e)
		{
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		return null;
	}

	public void send(TopicInfo topicInfo, String interfaceIp, byte[] bytes) throws Exception
	{
		this.sender.send(topicInfo, interfaceIp, bytes);
	}

	public void subscribe(TopicInfo topicInfo, String interfaceIp, IDataListener listener, int recvBufferSize)
	{
		this.receiver.subscribe(topicInfo, interfaceIp, listener, recvBufferSize);
	}
}