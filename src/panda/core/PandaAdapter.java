package panda.core;

import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;


//Do not subscribe to same group on two diff network cards in same adapter
//Do not subscribe to same group on two diff adapters
//TODO - change topic from short to string
public class PandaAdapter
{
	private static final Logger LOGGER = Logger.getLogger(PandaAdapter.class.getName());

	private final SelectorThread selectorThread;
	private final Receiver receiver;
	private final Sender sender;

	public PandaAdapter(int cacheSize) throws Exception
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

	public void send(PandaTopicInfo topicInfo, String interfaceIp, byte[] bytes) throws Exception
	{
		this.sender.send(topicInfo, interfaceIp, bytes);
	}

	public void subscribe(PandaTopicInfo topicInfo, String interfaceIp, PandaDataListener listener, int recvBufferSize)
	{
		this.receiver.subscribe(topicInfo, interfaceIp, listener, recvBufferSize);
	}
}