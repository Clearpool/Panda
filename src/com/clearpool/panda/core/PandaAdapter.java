package com.clearpool.panda.core;

import java.net.InetSocketAddress;
<<<<<<< HEAD
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.channels.DatagramChannel;
import java.nio.channels.ServerSocketChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
=======
import java.nio.channels.ServerSocketChannel;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
>>>>>>> dac1a1946521bffdeb7503d0dcdfe7edabe0ce74
import java.util.logging.Level;
import java.util.logging.Logger;

import com.codahale.metrics.MetricRegistry;

//Do not subscribe to same group on two diff network cards in same adapter
//Do not subscribe to same group on two diff adapters
public class PandaAdapter
{
	private static final Logger LOGGER = Logger.getLogger(PandaAdapter.class.getName());
<<<<<<< HEAD
	public static final Map<String, PandaAdapter> ALL_PANDA_ADAPTERS = Collections.synchronizedMap(new HashMap<String, PandaAdapter>());

	private final String name;
=======
	public static final List<PandaAdapter> ALL_PANDA_ADAPTERS = Collections.synchronizedList(new LinkedList<PandaAdapter>());

>>>>>>> dac1a1946521bffdeb7503d0dcdfe7edabe0ce74
	private final SelectorThread selectorThread;
	private final Receiver receiver;
	private final Sender sender;

	public PandaAdapter(int cacheSize) throws Exception
	{
<<<<<<< HEAD
		this(UUID.randomUUID().toString(), cacheSize);
	}

	public PandaAdapter(String name, int cacheSize) throws Exception
	{
		this.name = name;
		this.selectorThread = new SelectorThread();
		Pair<ServerSocketChannel, DatagramChannel> channelSocketPair = initChannelPair();
		this.receiver = new Receiver(this.selectorThread, channelSocketPair.getA().socket().getLocalPort());
		this.sender = new Sender(this.selectorThread, channelSocketPair.getA(), channelSocketPair.getB(), cacheSize);
		this.selectorThread.start();
		registerPandaAdapter();
	}

	private static Pair<ServerSocketChannel, DatagramChannel> initChannelPair() throws Exception
	{
		while (true)
		{
			try
			{
				// Create TCP Channel
				ServerSocketChannel tcpChannel = ServerSocketChannel.open();
				tcpChannel.configureBlocking(false);
				tcpChannel.bind(new InetSocketAddress(0));
				LOGGER.info("Binding on port " + tcpChannel.socket().getLocalPort());

				// Create UDP Channel
				DatagramChannel udpChannel = DatagramChannel.open(StandardProtocolFamily.INET);
				udpChannel.setOption(StandardSocketOptions.SO_REUSEADDR, Boolean.TRUE);
				udpChannel.setOption(StandardSocketOptions.IP_MULTICAST_TTL, Integer.valueOf(255));
				udpChannel.configureBlocking(false);
				udpChannel.bind(new InetSocketAddress(tcpChannel.socket().getLocalPort()));

				return new Pair<ServerSocketChannel, DatagramChannel>(tcpChannel, udpChannel);
			}
			catch (Exception e)
			{
				LOGGER.log(Level.SEVERE, e.getMessage(), e);
				Thread.sleep(1);
			}
		}
	}

	private void registerPandaAdapter()
	{
		ALL_PANDA_ADAPTERS.put(this.name, this);
=======
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
>>>>>>> dac1a1946521bffdeb7503d0dcdfe7edabe0ce74
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

<<<<<<< HEAD
	public void recordStats(MetricRegistry metricsRegistry)
	{
		this.receiver.recordStats(metricsRegistry, this.name);
		this.sender.recordStats(metricsRegistry, this.name);
	}

	Receiver getReceiver()
	{
		return this.receiver;
=======
	public void recordStats(MetricRegistry metricsRegistry, String prefix)
	{
		this.receiver.recordStats(metricsRegistry, prefix);
		this.sender.recordStats(metricsRegistry, prefix);
>>>>>>> dac1a1946521bffdeb7503d0dcdfe7edabe0ce74
	}
}