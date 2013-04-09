package panda.core;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import panda.utils.Utils;

public class GapRequestManager
{
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

	private boolean disabled;
	private boolean active;

	public GapRequestManager(SelectorThread selectorThread, String multicastGroup, String sourceIp, ChannelReceiveSequencer sequencer)
	{
		this.selectorThread = selectorThread;
		this.multicastGroup = multicastGroup;
		this.sourceIp = sourceIp;
		this.sequencer = sequencer;
		this.readBuffer = ByteBuffer.allocateDirect(2 * Utils.MAX_TCP_SIZE);

		this.socketChannel = null;
		this.request = null;
		this.timeOfRequest = 0;
		this.firstSequenceNumberRequested = 0;
		this.packetCountRequested = 0;
		this.responseHeaderReceived = false;
		this.responseFirstSequenceNumber = 0;
		this.responsePacketCount = 0;
		this.packetsRemainingToDeliver = 0;

		this.disabled = false;
		this.active = false;
	}

	public boolean sendGapRequest(int retransmissionPort, long firstSequenceNumber, int packetCount)
	{
		if (this.socketChannel == null)
		{
			this.active = true;
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
		byte[] bytes = new byte[Utils.RETRANSMISSION_REQUEST_HEADER_SIZE + this.multicastGroup.length()];
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		buffer.putLong(firstSequenceNumber);
		buffer.putInt(packetCount);
		buffer.put(this.multicastGroup.getBytes());
		buffer.rewind();

		this.timeOfRequest = System.currentTimeMillis();
		this.firstSequenceNumberRequested = firstSequenceNumber;
		this.packetCountRequested = packetCount;
		// LOGGER.info("Requesting " + this.packetCountRequested + " missed packets from source " + this.sequencer.getKey() + " starting with Seq. Num. " + this.firstSequenceNumberRequested + " for " + this.multicastGroup);

		return buffer;
	}

	// Called by selectorThread
	public ByteBuffer getGapRequest()
	{
		return this.request;
	}

	// Called by selectorThread
	public void processGapResponse(SocketChannel channel, SelectionKey key, ByteBuffer buffer)
	{
		this.readBuffer.put(buffer); // add to the end of whatever is remaining in bytebuffer
		this.readBuffer.flip();
		try
		{
			// Parse Header
			if (!this.responseHeaderReceived)
			{
				this.readBuffer.mark();
				if (this.readBuffer.remaining() >= Utils.RETRANSMISSION_RESPONSE_HEADER_SIZE)
				{
					long startSequenceNumber = this.readBuffer.getLong();
					int totalPackets = this.readBuffer.getInt();

					this.responseFirstSequenceNumber = startSequenceNumber;
					this.responsePacketCount = totalPackets;
					this.responseHeaderReceived = true;

					this.packetsRemainingToDeliver = this.responsePacketCount;
					// LOGGER.info("Recd " + this.responsePacketCount + " packets from " + this.sequencer.getKey() + " through retransmission for " + this.multicastGroup);

					// Potentially skip packets if request is not filled
					if (this.responsePacketCount == 0)
					{
						this.sequencer.getChannelReceiveInfo().deliverErrorToListeners(PandaErrorCode.RETRANSMISSION_RESPONSE_NONE, "Unable to retrieve missed packets from source=" + this.sequencer.getKey() + ". Skipping " + this.packetCountRequested + " packets.", null);
						long sequenceNumber = this.firstSequenceNumberRequested + this.packetCountRequested - 1;
						this.sequencer.skipPacketAndDequeue(sequenceNumber);
					}
					else if (this.responsePacketCount != this.packetCountRequested)
					{
						this.sequencer.getChannelReceiveInfo().deliverErrorToListeners(PandaErrorCode.RETRANSMISSION_RESPONSE_PARTIAL, "Unable to retrieve all missed packets from source=" + this.sequencer.getKey() + ". Skipping " + (this.responsePacketCount - this.packetCountRequested) + " packets.", null);
						this.sequencer.skipPacketAndDequeue(this.responseFirstSequenceNumber - 1);
					}
				}
				else
				{
					this.readBuffer.reset();
					this.readBuffer.compact();
					this.readBuffer.limit(this.readBuffer.capacity());
				}
			}

			// Parse Packets
			while (this.packetsRemainingToDeliver > 0)
			{
				this.readBuffer.mark();
				if (this.readBuffer.remaining() >= Utils.RETRANSMISSION_RESPONSE_PACKET_HEADER_SIZE)
				{
					int packetLength = this.readBuffer.getInt();
					if (this.readBuffer.remaining() >= packetLength)
					{
						byte[] bytes = new byte[packetLength];
						this.readBuffer.get(bytes);
						ByteBuffer packetBuffer = ByteBuffer.wrap(bytes);
						this.sequencer.getChannelReceiveInfo().dataReceived((InetSocketAddress) channel.socket().getRemoteSocketAddress(), packetBuffer);
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
				{
					this.readBuffer.reset();
					this.readBuffer.compact();
					return;
				}
			}

			if (this.packetsRemainingToDeliver == 0)
			{
				close(channel, key);
			}
		}
		catch (Exception e)
		{
			this.sequencer.getChannelReceiveInfo().deliverErrorToListeners(PandaErrorCode.EXCEPTION, e.getMessage(), e);
		}
	}

	private void close(SocketChannel channel, SelectionKey key) throws IOException
	{
		channel.close();
		key.cancel();
		close();
	}

	public void close()
	{
		this.readBuffer.clear();
		this.socketChannel = null;
		this.request = null;
		this.timeOfRequest = 0;
		this.firstSequenceNumberRequested = 0L;
		this.packetCountRequested = 0L;
		this.responseHeaderReceived = false;
		this.responseFirstSequenceNumber = 0L;
		this.responsePacketCount = 0;
		this.packetsRemainingToDeliver = 0;
		this.active = false;
		this.sequencer.closeRetransmissionManager();
	}

	public boolean isDisabled()
	{
		return this.disabled;
	}

	public void setDisabled(boolean disabled)
	{
		this.disabled = disabled;
	}

	public String getMulticastGroup()
	{
		return this.multicastGroup;
	}

	public boolean isActive()
	{
		return this.active;
	}

	public long getTimeOfRequest()
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
}