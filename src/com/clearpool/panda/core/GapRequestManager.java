package com.clearpool.panda.core;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.logging.Logger;

class GapRequestManager
{
	private static final Logger LOGGER = Logger.getLogger(GapRequestManager.class.getName());

	private final SelectorThread selectorThread;
	private final String multicastGroup;
	private final String sourceIp;
	private final ChannelReceiveSequencer sequencer;
	private final ByteBuffer readBuffer;

	private SocketChannel socketChannel;
	private ByteBuffer request;
	private long timeOfRequest;
	private long firstSequenceNumberRequested;
	private long packetCountRequested;
	private boolean responseHeaderReceived;
	private long responseFirstSequenceNumber;
	private int responsePacketCount;
	private int packetsRemainingToDeliver;

	GapRequestManager(SelectorThread selectorThread, String multicastGroup, String sourceIp, ChannelReceiveSequencer sequencer)
	{
		this.selectorThread = selectorThread;
		this.multicastGroup = multicastGroup;
		this.sourceIp = sourceIp;
		this.sequencer = sequencer;
		this.readBuffer = ByteBuffer.allocateDirect(2 * PandaUtils.MAX_TCP_SIZE);

		this.socketChannel = null;
		this.request = null;
		this.timeOfRequest = 0;
		this.firstSequenceNumberRequested = 0;
		this.packetCountRequested = 0;
		this.responseHeaderReceived = false;
		this.responseFirstSequenceNumber = 0;
		this.responsePacketCount = 0;
		this.packetsRemainingToDeliver = 0;
	}

	boolean sendGapRequest(int retransmissionPort, long firstSequenceNumber, int packetCount)
	{
		if (this.socketChannel == null)
		{
			this.socketChannel = getSocketChannel(this.sourceIp, retransmissionPort);
			this.selectorThread.registerTcpChannelAction(this.socketChannel, SelectionKey.OP_CONNECT, this);
			this.request = createGapRequest(firstSequenceNumber, packetCount);
		}
		return true;
	}

	private SocketChannel getSocketChannel(String remoteHost, int remotePort)
	{
		try
		{
			SocketChannel channel = SocketChannel.open();
			channel.configureBlocking(false);
			channel.connect(new InetSocketAddress(remoteHost, remotePort));
			return channel;
		}
		catch (Exception e)
		{
			this.sequencer.getChannelReceiveInfo().deliverErrorToListeners(PandaErrorCode.EXCEPTION, e.getMessage(), e);
		}
		return null;
	}

	private ByteBuffer createGapRequest(long firstSequenceNumber, int packetCount)
	{
		ByteBuffer buffer = ByteBuffer.allocate(PandaUtils.RETRANSMISSION_REQUEST_HEADER_SIZE + this.multicastGroup.length());
		buffer.putLong(firstSequenceNumber);
		buffer.putInt(packetCount);
		buffer.put((byte) this.multicastGroup.length());
		buffer.put(this.multicastGroup.getBytes());
		buffer.rewind();

		this.timeOfRequest = System.currentTimeMillis();
		this.firstSequenceNumberRequested = firstSequenceNumber;
		this.packetCountRequested = packetCount;

		return buffer;
	}

	// Called by selectorThread
	ByteBuffer getGapRequest()
	{
		return this.request;
	}

	// Called by selectorThread
	void processGapResponse(SocketChannel channel, SelectionKey key, ByteBuffer buffer)
	{
		// TODO - what if buffer.length is > remaining? - POSSIBLE scenario = after PACKET_LOSS_RETRANSMISSION_TIMEOUT
		this.readBuffer.put(buffer); // add to the end of whatever is remaining in bytebuffer
		this.readBuffer.flip();
		try
		{
			// Parse Header
			if (!this.responseHeaderReceived)
			{
				this.readBuffer.mark();
				if (this.readBuffer.remaining() >= PandaUtils.RETRANSMISSION_RESPONSE_HEADER_SIZE)
				{
					long startSequenceNumber = this.readBuffer.getLong();
					int totalPackets = this.readBuffer.getInt();

					this.responseFirstSequenceNumber = startSequenceNumber;
					this.responsePacketCount = totalPackets;
					this.responseHeaderReceived = true;

					this.packetsRemainingToDeliver = this.responsePacketCount;

					// Potentially skip packets if request is not filled
					if (this.responsePacketCount == 0)
					{
						this.sequencer.getChannelReceiveInfo().deliverErrorToListeners(PandaErrorCode.RETRANSMISSION_RESPONSE_NONE,
								"Unable to retrieve missed packets from source=" + this.sequencer.getKey() + ". Skipping " + this.packetCountRequested + " packets.", null);
						long sequenceNumber = this.firstSequenceNumberRequested + this.packetCountRequested - 1;
						this.sequencer.skipPacketAndDequeue(sequenceNumber);
					}
					else if (this.responsePacketCount != this.packetCountRequested)
					{
						this.sequencer.getChannelReceiveInfo().deliverErrorToListeners(
								PandaErrorCode.RETRANSMISSION_RESPONSE_PARTIAL,
								"Unable to retrieve missed packets from source=" + this.sequencer.getKey() + ". Skipping " + (this.responsePacketCount - this.packetCountRequested)
										+ " packets.", null);
						this.sequencer.skipPacketAndDequeue(this.responseFirstSequenceNumber - 1);
					}
				}
				else
				{
					this.readBuffer.reset();
					this.readBuffer.compact();
					return;
				}
			}

			// Parse Packets
			while (this.packetsRemainingToDeliver > 0)
			{
				this.readBuffer.mark();
				if (this.readBuffer.remaining() >= PandaUtils.RETRANSMISSION_RESPONSE_PACKET_HEADER_SIZE)
				{
					int packetLength = this.readBuffer.getInt();
					if (packetLength > 0)
					{
						if (this.readBuffer.remaining() >= packetLength)
						{
							byte[] bytes = new byte[packetLength];
							this.readBuffer.get(bytes);
							ByteBuffer packetBuffer = ByteBuffer.wrap(bytes);
							if (channel != null) this.sequencer.getChannelReceiveInfo().dataReceived((InetSocketAddress) channel.socket().getRemoteSocketAddress(), packetBuffer);
							this.packetsRemainingToDeliver--;
						}
						else
						{
							this.readBuffer.reset();
							this.readBuffer.compact();
							return;
						}
					}
					else
					// TODO - Remove the Logger once the issue is resolved
					{
						// Log readBuffer
						this.readBuffer.rewind();
						StringBuilder bufferSB = new StringBuilder();
						while (this.readBuffer.hasRemaining())
						{
							bufferSB.append(',');
							bufferSB.append(this.readBuffer.get());
						}
						LOGGER.severe("Received Negative/Zero Packet Length In Gap Response - ReadBuffer Bytes - " + bufferSB.toString());
						// Declare drop due to corruption
						this.sequencer.getChannelReceiveInfo().deliverErrorToListeners(
								PandaErrorCode.PACKET_LOSS_RETRANSMISSION_CORRUPTION,
								"Unable to retrieve missed packets due to possible data corruption from source=" + this.sequencer.getKey() + ". Skipping "
										+ this.packetsRemainingToDeliver + " packets.", null);
						long sequenceNumber = this.firstSequenceNumberRequested + this.packetCountRequested - 1;
						this.sequencer.skipPacketAndDequeue(sequenceNumber);
						// Induce channel close
						this.packetsRemainingToDeliver = 0;
					}
				}
				else
				{
					this.readBuffer.reset();
					this.readBuffer.compact();
					return;
				}
			}

			if (this.packetsRemainingToDeliver == 0)
			{
				close(channel, key, true);
			}
		}
		catch (Exception e)
		{
			this.sequencer.getChannelReceiveInfo().deliverErrorToListeners(PandaErrorCode.EXCEPTION, e.getMessage(), e);
			e.printStackTrace();
		}
	}

	private void close(SocketChannel channel, SelectionKey key, boolean successful) throws IOException
	{
		if (channel != null) channel.close();
		if (key != null) key.cancel();
		close(successful);
	}

	void close(boolean successful)
	{
		this.readBuffer.clear();
		this.socketChannel = null;
		this.request = null;
		this.sequencer.closeRequestManager(successful);
	}

	void setDisabled()
	{
		close(false);
		this.sequencer.disableRetransmissions();
	}

	String getMulticastGroup()
	{
		return this.multicastGroup;
	}

	long getTimeOfRequest()
	{
		return this.timeOfRequest;
	}

	long getFirstSequenceNumberRequested()
	{
		return this.firstSequenceNumberRequested;
	}

	long getPacketCountRequested()
	{
		return this.packetCountRequested;
	}

	boolean isResponseHeaderReceived()
	{
		return this.responseHeaderReceived;
	}

	long getResponseFirstSequenceNumber()
	{
		return this.responseFirstSequenceNumber;
	}

	int getResponsePacketCount()
	{
		return this.responsePacketCount;
	}

	int getPacketsRemainingToDeliver()
	{
		return this.packetsRemainingToDeliver;
	}
}