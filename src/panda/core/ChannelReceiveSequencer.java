package panda.core;

import java.nio.ByteBuffer;
import java.util.PriorityQueue;

import panda.core.containers.Packet;

public class ChannelReceiveSequencer
{
	private static final int OUT_OF_ORDER_PACKET_THRESHOLD = 20;
	private static final int QUEUE_GIVEUP_TIME = 3000;

	private final SelectorThread selectorThread;
	private final String key;
	private final String multicastGroup;
	private final String sourceIp;
	private final ChannelReceiveInfo channelReceiveInfo;
	private final int maxDroppedPacketsAllowed;
	private final PriorityQueue<Packet> queuedPackets;

	private long lastSequenceNumber;
	private GapRequestManager retransmissionManager;
	private long timeOfFirstQueuedPacket;
	private long packetsDropped;
	private long packetsLost;

	public ChannelReceiveSequencer(SelectorThread selectorThread, String key, String multicastGroup, String sourceIp, ChannelReceiveInfo channelReceiveInfo, int maxDroppedPacketsAllowed)
	{
		this.selectorThread = selectorThread;
		this.key = key;
		this.multicastGroup = multicastGroup;
		this.sourceIp = sourceIp;
		this.channelReceiveInfo = channelReceiveInfo;
		this.maxDroppedPacketsAllowed = maxDroppedPacketsAllowed;
		this.queuedPackets = new PriorityQueue<Packet>();

		this.lastSequenceNumber = 0;
		this.retransmissionManager = null;
		this.timeOfFirstQueuedPacket = 0;
		this.packetsDropped = 0;
		this.packetsLost = 0;
	}

	// Called by selectorThread
	public void packetReceived(boolean supportsRetranmissions, int retransmissionPort, long sequenceNumber, byte messageCount, ByteBuffer packetBuffer)
	{
		if (sequenceNumber == this.lastSequenceNumber + 1 || this.lastSequenceNumber == 0)
		{
			this.lastSequenceNumber = sequenceNumber;
			this.channelReceiveInfo.parseAndDeliverToListeners(messageCount, packetBuffer);
			dequeueQueuedPackets();
		}
		else if (sequenceNumber <= this.lastSequenceNumber)
		{
			// In case sender restarts - this is rare since the sender has to use the same sourcePort (randomly selected by OS) as the prior run
			if (sequenceNumber == 1)
			{
				this.lastSequenceNumber = sequenceNumber;
				this.channelReceiveInfo.parseAndDeliverToListeners(messageCount, packetBuffer);
				this.queuedPackets.clear();
				if (this.retransmissionManager != null)
				{
					this.retransmissionManager.close();
					this.retransmissionManager = null;
				}
			}
		}
		else
		{
			addPacketToQueue(sequenceNumber, messageCount, packetBuffer);
			boolean skipPacket = false;
			PandaErrorCode skipReason = null;
			if (shouldDeclareDrop())
			{
				long dropped = this.queuedPackets.peek().getSequenceNumber() - this.lastSequenceNumber - 1;
				this.packetsDropped += dropped;
				if (this.packetsDropped >= this.maxDroppedPacketsAllowed)
				{
					skipPacket = true;
					skipReason = PandaErrorCode.PACKET_LOSS_MAX_DROPS_EXCEEDED;
				}
				if (supportsRetranmissions)
				{
					skipPacket = !this.handleGap(retransmissionPort);
					if(skipPacket)
					{
						skipReason = PandaErrorCode.PACKET_LOSS_UNABLE_TO_HANDLE_GAP;
					}
				}
				else
				{
					skipPacket = true;
					skipReason = PandaErrorCode.PACKET_LOSS_RETRANSMISSION_UNSUPPORTED;
				}
			}

			if (skipPacket)
			{
				this.channelReceiveInfo.deliverErrorToListeners(skipReason,
						"Source=" + this.key + " Expected=" + (this.lastSequenceNumber + 1) + " Received=" + sequenceNumber + ". Retransmission not supported, skipping packets", null);
				long headSequenceNumber = this.queuedPackets.peek().getSequenceNumber();
				skipPacketAndDequeue(headSequenceNumber - 1);
			}
		}
	}

	private void dequeueQueuedPackets()
	{
		if (this.queuedPackets.size() == 0) return;
		while (this.queuedPackets.size() > 0)
		{
			Packet queuedPacket = this.queuedPackets.peek();
			if (queuedPacket.getSequenceNumber() <= this.lastSequenceNumber)
			{
				this.queuedPackets.remove();
			}
			else if (queuedPacket.getSequenceNumber() == this.lastSequenceNumber + 1)
			{
				queuedPacket = this.queuedPackets.remove();
				this.channelReceiveInfo.parseAndDeliverToListeners(queuedPacket.getMessageCount(), ByteBuffer.wrap(queuedPacket.getBytes()));
				this.lastSequenceNumber = queuedPacket.getSequenceNumber();
			}
			else
			{
				break;
			}
		}

		if (this.queuedPackets.size() == 0)
		{
			this.timeOfFirstQueuedPacket = 0;
		}
	}

	private boolean handleGap(int retransmissionPort)
	{
		if (this.retransmissionManager != null && this.retransmissionManager.isDisabled()) return false;
		if (this.retransmissionManager != null && this.retransmissionManager.isActive()) return true;

		Packet headPacket = this.queuedPackets.peek();
		if (headPacket.getSequenceNumber() > this.lastSequenceNumber + 1)
		{
			if (this.retransmissionManager == null)
			{
				this.retransmissionManager = new GapRequestManager(this.selectorThread, this.multicastGroup, this.sourceIp, this);
			}

			int packetCount = (int) (headPacket.getSequenceNumber() - this.lastSequenceNumber - 1);
			return this.retransmissionManager.sendGapRequest(retransmissionPort, this.lastSequenceNumber + 1, packetCount);
		}
		return false;
	}

	private void addPacketToQueue(long sequenceNumber, byte messageCount, ByteBuffer packetBuffer)
	{
		byte[] bytes = new byte[packetBuffer.remaining()];
		packetBuffer.get(bytes);
		Packet packet = new Packet(sequenceNumber, messageCount, bytes);
		this.queuedPackets.add(packet);
		if (this.queuedPackets.size() == 1)
		{
			this.timeOfFirstQueuedPacket = System.currentTimeMillis();
		}
	}

	private boolean shouldDeclareDrop()
	{
		if (this.packetsDropped >= this.maxDroppedPacketsAllowed) return true;
		if (this.queuedPackets.size() > OUT_OF_ORDER_PACKET_THRESHOLD) return true;
		if (System.currentTimeMillis() - this.timeOfFirstQueuedPacket > QUEUE_GIVEUP_TIME) return true;
		return false;
	}

	public void skipPacketAndDequeue(long sequenceNumber)
	{
		long skipped = sequenceNumber - this.lastSequenceNumber;
		this.packetsLost += skipped;
		this.lastSequenceNumber = sequenceNumber;
		dequeueQueuedPackets();
	}

	// Called by selectorThread
	public ChannelReceiveInfo getChannelReceiveInfo()
	{
		return this.channelReceiveInfo;
	}

	public void closeRetransmissionManager()
	{
		this.retransmissionManager = null;
	}

	public String getKey()
	{
		return this.key;
	}

	int getQueueSize()
	{
		return this.queuedPackets.size();
	}

	GapRequestManager getGapRequestManager()
	{
		return this.retransmissionManager;
	}

	long getLastSequenceNumber()
	{
		return this.lastSequenceNumber;
	}

	long getPacketsDropped()
	{
		return this.packetsDropped;
	}

	long getPacketsLost()
	{
		return this.packetsLost;
	}
}