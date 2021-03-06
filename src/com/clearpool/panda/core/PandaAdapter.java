package com.clearpool.panda.core;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.channels.DatagramChannel;
import java.nio.channels.ServerSocketChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;

import com.codahale.metrics.MetricRegistry;

//Do not subscribe to same group on two diff network cards in same adapter
//Do not subscribe to same group on two diff adapters
public class PandaAdapter
{
	private static final Logger LOGGER = Logger.getLogger(PandaAdapter.class.getName());
	public static final Map<String, PandaAdapter> ALL_PANDA_ADAPTERS = Collections.synchronizedMap(new HashMap<String, PandaAdapter>());

	private final String name;
	private final SelectorThread selectorThread;
	private final Receiver receiver;
	private final Sender sender;

	public PandaAdapter(int cacheSize) throws Exception
	{
		this(UUID.randomUUID().toString(), cacheSize, null);
	}

	public PandaAdapter(String name, int cacheSize, PandaProperties properties) throws Exception
	{
		this.name = name;
		properties = (properties == null) ? new PandaProperties() : properties;
		this.selectorThread = new SelectorThread(properties);
		Pair<ServerSocketChannel, DatagramChannel> channelSocketPair = initChannelPair(this.name);
		this.receiver = new Receiver(this.selectorThread, channelSocketPair.getA().socket().getLocalPort(), properties);
		this.sender = new Sender(this.selectorThread, channelSocketPair.getA(), channelSocketPair.getB(), cacheSize, properties);
		this.selectorThread.start();
		registerPandaAdapter();
	}

	private static Pair<ServerSocketChannel, DatagramChannel> initChannelPair(String name) throws Exception
	{
		while (true)
		{
			try
			{
				// Create TCP Channel
				ServerSocketChannel tcpChannel = ServerSocketChannel.open();
				tcpChannel.configureBlocking(false);
				tcpChannel.bind(new InetSocketAddress(0));
				LOGGER.info(name + " - Binding on port " + tcpChannel.socket().getLocalPort());

				// Create UDP Channel
				DatagramChannel udpChannel = DatagramChannel.open(StandardProtocolFamily.INET);
				udpChannel.setOption(StandardSocketOptions.SO_REUSEADDR, Boolean.TRUE);
				udpChannel.setOption(StandardSocketOptions.IP_MULTICAST_TTL, Integer.valueOf(255));
				udpChannel.setOption(StandardSocketOptions.SO_SNDBUF, Integer.valueOf(1 << 25));
				udpChannel.configureBlocking(false);
				udpChannel.bind(new InetSocketAddress(tcpChannel.socket().getLocalPort()));

				return new Pair<ServerSocketChannel, DatagramChannel>(tcpChannel, udpChannel);
			}
			catch (Exception e)
			{
				LOGGER.warning("Trying again in 1ms");
				Thread.sleep(1);
			}
		}
	}

	private void registerPandaAdapter()
	{
		ALL_PANDA_ADAPTERS.put(this.name, this);
	}

	public void send(String topic, String ip, int port, String multicastGroup, InetAddress interfaceIp, byte[] bytes) throws Exception
	{
		if (multicastGroup == null) multicastGroup = PandaUtils.getMulticastGroup(ip, port);
		this.sender.send(topic, ip, port, multicastGroup, interfaceIp, bytes);
	}

	// Skipping will only work at the ip/port level.
	public PandaDataListener subscribe(String topic, String ip, int port, String multicastGroup, InetAddress interfaceIp, PandaDataListener listener, int recvBufferSize,
			boolean skipGaps)
	{
		if (multicastGroup == null) multicastGroup = PandaUtils.getMulticastGroup(ip, port);
		return this.receiver.subscribe(topic, ip, port, multicastGroup, interfaceIp, listener, recvBufferSize, skipGaps);
	}

	public void recordStats(MetricRegistry metricsRegistry)
	{
		this.receiver.recordStats(metricsRegistry, this.name);
		this.sender.recordStats(metricsRegistry, this.name);
	}

	Receiver getReceiver()
	{
		return this.receiver;
	}
}