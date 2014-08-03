package com.clearpool.panda.core;

import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.codahale.metrics.MetricRegistry;

//Do not subscribe to same group on two diff network cards in same adapter
//Do not subscribe to same group on two diff adapters
public class PandaAdapter
{
	private static final Logger LOGGER = Logger.getLogger(PandaAdapter.class.getName());
	public static final List<PandaAdapter> ALL_PANDA_ADAPTERS = Collections.synchronizedList(new LinkedList<PandaAdapter>());

	private final SelectorThread selectorThread;
	private final Receiver receiver;
	private final Sender sender;

	// TODO - add name to PandaAdapter
	public PandaAdapter(int cacheSize) throws Exception
	{
		this.selectorThread = new SelectorThread();
		ServerSocketChannel channel = initChannels();
		this.receiver = new Receiver(this.selectorThread, channel.socket().getLocalPort());
		this.sender = new Sender(this.selectorThread, channel, cacheSize);
		this.selectorThread.start();
		ALL_PANDA_ADAPTERS.add(this);
	}

	private static ServerSocketChannel initChannels() throws Exception
	{
		try
		{
			ServerSocketChannel channel = ServerSocketChannel.open();
			channel.configureBlocking(false);
			channel.bind(new InetSocketAddress(0));
			LOGGER.info("Binding on port " + channel.socket().getLocalPort());
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

	// TODO - prefix should be adaptername
	public void recordStats(MetricRegistry metricsRegistry, String prefix)
	{
		this.receiver.recordStats(metricsRegistry, prefix);
		this.sender.recordStats(metricsRegistry, prefix);
	}
}