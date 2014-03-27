package com.clearpool.panda.core;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

//Do not subscribe to same group on two diff network cards in same adapter
//Do not subscribe to same group on two diff adapters
public class PandaAdapter
{
	private static final Logger LOGGER = Logger.getLogger(PandaAdapter.class.getName());

	private final SelectorThread selectorThread;
	private final Receiver receiver;
	private final Sender sender;

	public PandaAdapter(int cacheSize) throws Exception
	{
		this.selectorThread = new SelectorThread();
		ServerSocketChannel channel = initChannels();
		this.receiver = new Receiver(this.selectorThread, channel.socket().getLocalPort());
		this.sender = new Sender(this.selectorThread, channel, cacheSize);
		this.selectorThread.start();
	}

	private ServerSocketChannel initChannels() throws Exception
	{
		while (true)
		{
			try
			{
				ServerSocketChannel channel = getServerSocketChannel();
				this.selectorThread.createOutDatagramChannel(channel.socket().getLocalPort());
				LOGGER.info("Binding on port " + channel.socket().getLocalPort());
				return channel;
			}
			catch (IOException e)
			{
				LOGGER.info(e.getMessage());
			}
		}
	}

	private static ServerSocketChannel getServerSocketChannel()
	{
		try
		{
			ServerSocketChannel channel = ServerSocketChannel.open();
			channel.configureBlocking(false);
			channel.bind(new InetSocketAddress(0));
			return channel;
		}
		catch (Exception e)
		{
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		return null;
	}

	public void send(String topic, String ip, int port, String multicastGroup, String interfaceIp, byte[] bytes) throws Exception
	{
		if (multicastGroup == null) multicastGroup = PandaUtils.getMulticastGroup(interfaceIp, port);
		this.sender.send(topic, ip, port, multicastGroup, interfaceIp, bytes);
	}

	// Skipping will only work at the ip/port level.
	public void subscribe(String topic, String ip, int port, String multicastGroup, String interfaceIp, PandaDataListener listener, int recvBufferSize, boolean skipGaps)
	{
		if (multicastGroup == null) multicastGroup = PandaUtils.getMulticastGroup(interfaceIp, port);
		this.receiver.subscribe(topic, ip, port, multicastGroup, interfaceIp, listener, recvBufferSize, skipGaps);
	}
}