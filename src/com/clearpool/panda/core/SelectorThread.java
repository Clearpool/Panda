package com.clearpool.panda.core;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AbstractSelectableChannel;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

class SelectorThread extends Thread
{
	private static final Logger LOGGER = Logger.getLogger(SelectorThread.class.getName());

	private final Selector selector;
	private final ByteBuffer udpBuffer;
	private final ByteBuffer tcpBuffer;
	private final Map<String, DatagramChannel> inDatagramChannels;
	private final ConcurrentLockFreeQueue<SelectorActionable> selectorActionQueue;
	private final long pegStartTime;
	private final long pegEndTime;

	SelectorThread(PandaProperties properties) throws IOException
	{
		this.selector = Selector.open();
		this.udpBuffer = ByteBuffer.allocateDirect(PandaUtils.MAX_UDP_SIZE);
		this.tcpBuffer = ByteBuffer.allocateDirect(PandaUtils.MAX_TCP_SIZE);
		this.inDatagramChannels = new HashMap<String, DatagramChannel>();
		this.selectorActionQueue = new ConcurrentLockFreeQueue<SelectorActionable>();
		this.pegStartTime = properties.getLongProperty(PandaProperties.PEG_SELECTOR_START_TIME, 0);
		this.pegEndTime = properties.getLongProperty(PandaProperties.PEG_SELECTOR_END_TIME, 0);
	}

	@Override
	public void run()
	{
		final Queue<SelectorActionable> activeSelectorActionQueue = new ArrayDeque<SelectorActionable>(1024);
		while (!Thread.currentThread().isInterrupted())
		{
			try
			{
				// Pull all actions + Service them
				SelectorActionable next = this.selectorActionQueue.take();
				while (next != null)
				{
					activeSelectorActionQueue.add(next);
					next = this.selectorActionQueue.take();
				}
				serviceEachSelectorAction(activeSelectorActionQueue);

				// Do selection
				int selectedKeyCount = select();
				if (selectedKeyCount > 0)
				{
					Set<SelectionKey> selectedKeys = this.selector.selectedKeys();
					Iterator<SelectionKey> selectionKeyIterator = selectedKeys.iterator();
					while (selectionKeyIterator.hasNext())
					{
						SelectionKey selectedKey = selectionKeyIterator.next();
						selectionKeyIterator.remove();

						if (selectedKey.channel() instanceof DatagramChannel)
						{
							handleMulticastSelection(selectedKey);
						}
						else
						{
							handleTcpSelection(selectedKey);
						}
					}
				}
			}
			catch (Exception e)
			{
				LOGGER.log(Level.SEVERE, e.getMessage(), e);
			}
		}
		LOGGER.warning("SelectorThread has been interrupted.  Stopping selection for selector");
	}

	private int select() throws IOException
	{
		if (shouldPegCpu())
		{
			return this.selector.selectNow();
		}
		return this.selector.select();
	}

	private boolean shouldPegCpu()
	{
		// Never pegging
		if (this.pegStartTime == 0)
		{
			return false;
		}

		long now = System.currentTimeMillis();

		// Candidate for pegging - Before peg start time
		if (now < this.pegStartTime)
		{
			return false;
		}

		// Candidate for pegging - After peg start time, but before end time
		if (this.pegEndTime == 0 || now <= this.pegEndTime)
		{
			return true;
		}

		// Candidate for pegging - After peg start time, and after end time
		return false;
	}

	private void serviceEachSelectorAction(Queue<SelectorActionable> activeActionQueue)
	{
		SelectorActionable selectorActionable = activeActionQueue.poll();
		while (selectorActionable != null)
		{
			int action = selectorActionable.getAction();
			if (action == SelectorActionable.SEND_MULTICAST)
			{
				sendMulticastData((ChannelSendDetail) selectorActionable, activeActionQueue);
			}
			else if (action == SelectorActionable.REGISTER_MULTICAST_READ)
			{
				registerMulticastChannel((MulticastRegistration) selectorActionable);
			}
			else if (action == SelectorActionable.REGISTER_TCP_ACTION)
			{
				registerTcpChannel((TcpRegistration) selectorActionable);
			}
			selectorActionable = activeActionQueue.poll();
		}
	}

	private void handleTcpSelection(SelectionKey selectedKey)
	{
		if (!selectedKey.isValid()) return;
		if (selectedKey.isAcceptable())
		{
			try
			{
				SocketChannel socketChannel = ((ServerSocketChannel) selectedKey.channel()).accept();
				socketChannel.configureBlocking(false);
				socketChannel.register(this.selector, SelectionKey.OP_READ, selectedKey.attachment());
			}
			catch (Exception e)
			{
				LOGGER.log(Level.SEVERE, e.getMessage(), e);
			}
		}
		else if (selectedKey.isReadable())
		{
			SocketChannel channel = (SocketChannel) selectedKey.channel();
			boolean successfulRead = readTcpChannel(channel, selectedKey);

			Object attachment = selectedKey.attachment();
			if (attachment instanceof Sender)
			{
				Sender sender = (Sender) attachment;
				if (successfulRead)
				{
					sender.processGapRequest(channel, this.tcpBuffer);
				}
				else
				{
					sender.close();
				}
			}
			else if (attachment instanceof GapRequestManager)
			{
				GapRequestManager gapManager = (GapRequestManager) attachment;
				if (successfulRead)
				{
					gapManager.processGapResponse(channel, selectedKey, this.tcpBuffer);
				}
				else
				{
					gapManager.close(false);
				}
			}
		}
		else if (selectedKey.isWritable())
		{
			Object attachment = selectedKey.attachment();
			if (attachment instanceof GapRequestManager)
			{
				try
				{
					GapRequestManager grm = (GapRequestManager) attachment;
					ByteBuffer gapRequest = grm.getGapRequest();
					if (gapRequest != null)
					{
						SocketChannel channel = (SocketChannel) selectedKey.channel();
						if (LOGGER.getLevel() == Level.FINE)
						{
							LOGGER.log(
									Level.FINE,
									"Sending GapRequest to " + channel.getRemoteAddress() + " with multicastGroup " + grm.getMulticastGroup() + " for "
											+ grm.getPacketCountRequested() + " packets starting with sequenceNumber " + grm.getFirstSequenceNumberRequested());
						}
						channel.write(gapRequest);
						if (gapRequest.remaining() == 0)
						{
							selectedKey.interestOps(SelectionKey.OP_READ);
						}
					}
					// This happens when the request times out
					else
					{
						try
						{
							((SocketChannel) selectedKey.channel()).close();
						}
						catch (IOException e1)
						{
							LOGGER.log(Level.SEVERE, e1.getMessage(), e1);
						}
						selectedKey.cancel();
					}
				}
				catch (Exception e)
				{
					LOGGER.log(Level.SEVERE, e.getMessage(), e);
					try
					{
						((SocketChannel) selectedKey.channel()).close();
					}
					catch (IOException e1)
					{
						LOGGER.log(Level.SEVERE, e1.getMessage(), e1);
					}
					selectedKey.cancel();
				}
			}
			else if (attachment instanceof GapResponseManager)
			{
				((GapResponseManager) attachment).sendResponse(selectedKey);
			}
		}
		else if (selectedKey.isConnectable())
		{
			try
			{
				if (((SocketChannel) selectedKey.channel()).finishConnect())
				{
					selectedKey.interestOps(SelectionKey.OP_WRITE);
				}
			}
			catch (Exception e)
			{
				LOGGER.log(Level.SEVERE, e.getMessage(), e);

				GapRequestManager gapManager = (GapRequestManager) selectedKey.attachment();
				LOGGER.severe("Failed to establish TCP connection for re-transmission. Disabling future re-transmission attempts for " + gapManager.getMulticastGroup()
						+ " on the receiver side.");
				gapManager.setDisabled();
				try
				{
					((SocketChannel) selectedKey.channel()).close();
				}
				catch (IOException e1)
				{
					LOGGER.log(Level.SEVERE, e1.getMessage(), e1);
				}
				selectedKey.cancel();
			}
		}
	}

	private boolean readTcpChannel(SocketChannel channel, SelectionKey selectedKey)
	{
		this.tcpBuffer.clear();
		try
		{
			int numBytesRead = channel.read(this.tcpBuffer);
			if (numBytesRead == -1)
			{
				channel.close();
				selectedKey.cancel();
				return false;
			}
			this.tcpBuffer.flip();
		}
		catch (IOException e)
		{
			try
			{
				channel.close();
			}
			catch (IOException e1)
			{
				LOGGER.log(Level.SEVERE, e.getMessage(), e);
			}
			selectedKey.cancel();
			return false;
		}
		return true;
	}

	private void handleMulticastSelection(SelectionKey selectedKey)
	{
		if (selectedKey.isReadable())
		{
			try
			{
				this.udpBuffer.clear();
				InetSocketAddress sourceAddress = (InetSocketAddress) ((DatagramChannel) selectedKey.channel()).receive(this.udpBuffer);
				this.udpBuffer.flip();
				((ChannelReceiveInfo) selectedKey.attachment()).dataReceived(sourceAddress, this.udpBuffer);
			}
			catch (Exception e)
			{
				LOGGER.log(Level.SEVERE, e.getMessage(), e);
			}
		}
	}

	static void sendMulticastData(ChannelSendDetail channelSendDetail, Queue<SelectorActionable> actionQueue)
	{
		SelectorActionable nextSelectorActionable = actionQueue.peek();
		ChannelSendInfo channelSendInfo = channelSendDetail.getChannelSendInfo();
		String messageTopic = channelSendDetail.getMessageTopic();
		int messageTopicLength = messageTopic.length();
		byte[] messageBytes = channelSendDetail.getMessageBytes();
		int messageBytesLength = messageBytes.length;
		byte supportsRetransmissions = channelSendInfo.supportsRetransmissions();
		long sequenceNumber = channelSendInfo.incrementAndGetSequenceNumber();
		byte messageCount = 1;
		byte[] messageTopicBytes = messageTopic.getBytes();
		ByteBuffer multicastBuffer;

		if (messageBytesLength + messageTopicLength > PandaUtils.PANDA_PACKET_MESSAGE_PAYLOAD_SIZE || nextSelectorActionable == null
				|| nextSelectorActionable.getAction() != SelectorActionable.SEND_MULTICAST || ((ChannelSendDetail) nextSelectorActionable).getChannelSendInfo() != channelSendInfo)
		{
			multicastBuffer = ByteBuffer.allocate(PandaUtils.PACKET_HEADER_SIZE + PandaUtils.MESSAGE_HEADER_FIXED_SIZE + messageTopicLength + messageBytesLength);
			multicastBuffer.put(PandaUtils.PACKET_HEADER_SIZE);
			multicastBuffer.put(supportsRetransmissions);
			multicastBuffer.putLong(sequenceNumber);
			multicastBuffer.put(messageCount);
			multicastBuffer.put((byte) messageTopicLength);
			multicastBuffer.put(messageTopicBytes);
			multicastBuffer.putShort((short) messageBytesLength);
			multicastBuffer.put(messageBytes);
		}
		else
		{
			ByteBuffer payloadBuffer = ByteBuffer.allocate(PandaUtils.PANDA_PACKET_PAYLOAD_SIZE);
			payloadBuffer.put((byte) messageTopicLength);
			payloadBuffer.put(messageTopicBytes);
			payloadBuffer.putShort((short) messageBytesLength);
			payloadBuffer.put(messageBytes);
			while (messageCount < Byte.MAX_VALUE && nextSelectorActionable != null && nextSelectorActionable.getAction() == SelectorActionable.SEND_MULTICAST)
			{
				channelSendDetail = (ChannelSendDetail) nextSelectorActionable;
				if (channelSendDetail.getChannelSendInfo() != channelSendInfo) break;
				messageTopic = channelSendDetail.getMessageTopic();
				messageBytes = channelSendDetail.getMessageBytes();
				messageTopicLength = messageTopic.length();
				messageTopicBytes = messageTopic.getBytes();
				messageBytesLength = messageBytes.length;
				if (payloadBuffer.remaining() < PandaUtils.MESSAGE_HEADER_FIXED_SIZE + messageTopicLength + messageBytesLength) break;
				actionQueue.poll();
				payloadBuffer.put((byte) messageTopicLength);
				payloadBuffer.put(messageTopicBytes);
				payloadBuffer.putShort((short) messageBytesLength);
				payloadBuffer.put(messageBytes);
				messageCount++;
				nextSelectorActionable = actionQueue.peek();
			}
			multicastBuffer = ByteBuffer.allocate(PandaUtils.PACKET_HEADER_SIZE + payloadBuffer.position());
			multicastBuffer.put(PandaUtils.PACKET_HEADER_SIZE);
			multicastBuffer.put(supportsRetransmissions);
			multicastBuffer.putLong(sequenceNumber);
			multicastBuffer.put(messageCount);
			multicastBuffer.put(payloadBuffer.array(), 0, payloadBuffer.position());
		}
		try
		{
			multicastBuffer.flip();
			channelSendInfo.sendToChannel(multicastBuffer);
		}
		catch (IOException e)
		{
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	private void registerMulticastChannel(MulticastRegistration registration)
	{
		try
		{
			DatagramChannel channel = registration.getChannel();
			channel.register(this.selector, SelectionKey.OP_READ, registration.getAttachment());
			channel.join(InetAddress.getByName(registration.getIp()), channel.getOption(StandardSocketOptions.IP_MULTICAST_IF));
		}
		catch (Exception e)
		{
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	private void registerTcpChannel(TcpRegistration registration)
	{
		try
		{
			registration.getChannel().register(this.selector, registration.getInterestOps(), registration.getAttachment());
		}
		catch (Exception e)
		{
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	void sendToMulticastChannel(ChannelSendInfo sendInfo, String topic, byte[] bytes)
	{
		try
		{
			addToActionQueue(new ChannelSendDetail(sendInfo, topic, bytes));
		}
		catch (Exception e)
		{
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	// Will be called synchronously
	void subscribeToMulticastChannel(String ip, int port, String multicastGroup, InetAddress interfaceIp, ChannelReceiveInfo receiverInfo, int recvBufferSize)
	{
		try
		{
			DatagramChannel channel = this.inDatagramChannels.get(multicastGroup);
			if (channel == null)
			{
				channel = createDatagramChannel(interfaceIp);
				if (System.getProperty("os.name").toLowerCase().contains("windows"))
				{
					channel.bind(new InetSocketAddress(port));
				}
				else
				{
					channel.bind(new InetSocketAddress(InetAddress.getByName(ip), port));
				}
				channel.setOption(StandardSocketOptions.SO_RCVBUF, Integer.valueOf(recvBufferSize));
				this.inDatagramChannels.put(multicastGroup, channel);
				addToActionQueue(new MulticastRegistration(channel, ip, receiverInfo));
			}
		}
		catch (Exception e)
		{
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	void registerTcpChannelAction(AbstractSelectableChannel channel, int interestOps, Object attachment)
	{
		addToActionQueue(new TcpRegistration(channel, interestOps, attachment));
	}

	private static DatagramChannel createDatagramChannel()
	{
		try
		{
			DatagramChannel channel = DatagramChannel.open(StandardProtocolFamily.INET);
			channel.setOption(StandardSocketOptions.SO_REUSEADDR, Boolean.TRUE);
			channel.configureBlocking(false);
			return channel;
		}
		catch (Exception e)
		{
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		return null;
	}

	private static DatagramChannel createDatagramChannel(InetAddress interfaceIp)
	{
		try
		{
			DatagramChannel channel = createDatagramChannel();
			channel.setOption(StandardSocketOptions.IP_MULTICAST_IF, NetworkInterface.getByInetAddress(interfaceIp));
			return channel;
		}
		catch (Exception e)
		{
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		return null;
	}

	private void addToActionQueue(SelectorActionable action)
	{
		this.selectorActionQueue.offer(action);
		if (!shouldPegCpu()) this.selector.wakeup();
	}

	@SuppressWarnings("static-method")
	protected boolean shouldMakeConnections()
	{
		return true;
	}
}