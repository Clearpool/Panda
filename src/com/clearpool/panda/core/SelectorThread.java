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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
	private final List<SelectorActionable> selectorActionQueue;

	private DatagramChannel outDatagramChannel;

	public SelectorThread() throws IOException
	{
		this.selector = Selector.open();
		this.udpBuffer = ByteBuffer.allocateDirect(PandaUtils.MTU_SIZE);
		this.tcpBuffer = ByteBuffer.allocateDirect(PandaUtils.MAX_TCP_SIZE);
		this.inDatagramChannels = new HashMap<String, DatagramChannel>();
		this.selectorActionQueue = new LinkedList<>();

		this.outDatagramChannel = null;
	}

	public void createOutDatagramChannel(int mcBindPort)
	{
		try
		{
			this.outDatagramChannel = createDatagramChannel();
			this.outDatagramChannel.bind(new InetSocketAddress(mcBindPort));
		}
		catch (IOException e)
		{
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	@Override
	public void run()
	{
		final List<SelectorActionable> activeSelectorActionQueue = new LinkedList<>();

		while (!Thread.currentThread().isInterrupted())
		{
			try
			{
				// Check for actions
				synchronized (this.selectorActionQueue)
				{
					moveActionQueue(activeSelectorActionQueue);
				}

				// Service each action
				serviveEachSelectorAction(activeSelectorActionQueue);

				// Do selection
				int selectedKeyCount = this.selector.select();
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

	private void moveActionQueue(List<SelectorActionable> activeActionQueue)
	{
		if (!this.selectorActionQueue.isEmpty())
		{
			// Move to temporary queue
			Iterator<SelectorActionable> actionQueueIterator = this.selectorActionQueue.iterator();
			while (actionQueueIterator.hasNext())
			{
				SelectorActionable action = actionQueueIterator.next();
				activeActionQueue.add(action);
				actionQueueIterator.remove();
			}
		}
	}

	private void serviveEachSelectorAction(List<SelectorActionable> activeActionQueue)
	{
		if (!activeActionQueue.isEmpty())
		{
			Iterator<SelectorActionable> activeActionQueueIterator = activeActionQueue.iterator();
			while (activeActionQueueIterator.hasNext())
			{
				SelectorActionable action = activeActionQueueIterator.next();
				activeActionQueueIterator.remove();
				if (action.getAction() == SelectorActionable.SEND_MULTICAST)
				{
					sendMulticastData(action);
				}
				else if (action.getAction() == SelectorActionable.REGISTER_MULTICAST_READ)
				{
					registerMulticastChannel(action);
				}
				else if (action.getAction() == SelectorActionable.REGISTER_TCP_ACTION)
				{
					registerTcpChannel(action);
				}
			}
		}
	}

	private void handleTcpSelection(SelectionKey selectedKey)
	{
		if (!selectedKey.isValid()) return;
		if (selectedKey.isAcceptable())
		{
			try
			{
				ServerSocketChannel channel = (ServerSocketChannel) selectedKey.channel();
				SocketChannel socketChannel = channel.accept();
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
					GapRequestManager gapManager = (GapRequestManager) attachment;
					ByteBuffer outBuffer = gapManager.getGapRequest();
					SocketChannel channel = (SocketChannel) selectedKey.channel();
					channel.write(outBuffer);
					if (outBuffer.remaining() == 0)
					{
						selectedKey.interestOps(SelectionKey.OP_READ);
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
				GapResponseManager response = (GapResponseManager) attachment;
				response.sendResponse(selectedKey);
			}
		}
		else if (selectedKey.isConnectable())
		{
			try
			{
				SocketChannel channel = (SocketChannel) selectedKey.channel();
				if (channel.finishConnect())
				{
					selectedKey.interestOps(SelectionKey.OP_WRITE);
				}
			}
			catch (Exception e)
			{
				LOGGER.log(Level.SEVERE, e.getMessage(), e);

				GapRequestManager gapManager = (GapRequestManager) selectedKey.attachment();
				LOGGER.severe("Failed to establish TCP connection for re-transmission. Disabling future re-transmission attempts for " + gapManager.getMulticastGroup() + " on the receiver side.");
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
				ChannelReceiveInfo attachment = (ChannelReceiveInfo) selectedKey.attachment();
				attachment.dataReceived(sourceAddress, this.udpBuffer);
			}
			catch (Exception e)
			{
				LOGGER.log(Level.SEVERE, e.getMessage(), e);
			}
		}
	}

	private static void sendMulticastData(SelectorActionable action)
	{
		ChannelSendInfo sendInfo = (ChannelSendInfo) action;
		synchronized (sendInfo)
		{
			try
			{
				sendInfo.sendToChannel();
			}
			catch (Exception e)
			{
				LOGGER.log(Level.SEVERE, e.getMessage(), e);
			}
		}
	}

	private void registerMulticastChannel(SelectorActionable action)
	{
		MulticastRegistration registration = (MulticastRegistration) action;
		try
		{
			registration.getChannel().register(this.selector, SelectionKey.OP_READ, registration.getAttachment());
			registration.getChannel().join(InetAddress.getByName(registration.getIp()), registration.getChannel().getOption(StandardSocketOptions.IP_MULTICAST_IF));

		}
		catch (Exception e)
		{
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	private void registerTcpChannel(SelectorActionable action)
	{
		try
		{
			TcpRegistration registration = (TcpRegistration) action;
			registration.getChannel().register(this.selector, registration.getInterestOps(), registration.getAttachment());
		}
		catch (Exception e)
		{
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	// Will be called synchronously
	public void sendToMulticastChannel(ChannelSendInfo sendInfo)
	{
		try
		{
			sendInfo.setChannel(this.outDatagramChannel);
			addToActionQueue(sendInfo);
			this.selector.wakeup();
		}
		catch (Exception e)
		{
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	// Will be called synchronously
	public void subscribeToMulticastChannel(String ip, int port, String multicastGroup, String interfaceIp, ChannelReceiveInfo receiverInfo, int recvBufferSize)
	{
		try
		{
			DatagramChannel channel = this.inDatagramChannels.get(multicastGroup);
			if (channel == null)
			{
				channel = createDatagramChannel(interfaceIp);
				if(System.getProperty("os.name").toLowerCase().contains("windows"))
				{
					channel.bind(new InetSocketAddress(port));
				}
				else
				{
					channel.bind(new InetSocketAddress(InetAddress.getByName(ip), port));
				}
				channel.setOption(StandardSocketOptions.SO_RCVBUF, Integer.valueOf(recvBufferSize));
				this.inDatagramChannels.put(multicastGroup, channel);
				MulticastRegistration registration = new MulticastRegistration(channel, ip, port, receiverInfo);
				addToActionQueue(registration);
				this.selector.wakeup();
			}
		}
		catch (Exception e)
		{
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	public void registerTcpChannelAction(AbstractSelectableChannel channel, int interestOps, Object attachment)
	{
		TcpRegistration registration = new TcpRegistration(channel, interestOps, attachment);
		addToActionQueue(registration);
		this.selector.wakeup();
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

	private static DatagramChannel createDatagramChannel(String interfaceIp)
	{
		try
		{
			DatagramChannel channel = createDatagramChannel();
			channel.setOption(StandardSocketOptions.IP_MULTICAST_IF, NetworkInterface.getByInetAddress(InetAddress.getByName(interfaceIp)));
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
		synchronized (this.selectorActionQueue)
		{
			this.selectorActionQueue.add(action);
		}
	}
}