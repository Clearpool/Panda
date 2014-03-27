package com.clearpool.panda.core;

import java.nio.ByteBuffer;
import java.util.PriorityQueue;

class ChannelReceiveSequencer
{
	private static final int OUT_OF_ORDER_PACKET_THRESHOLD = 20;
	private static final int QUEUE_GIVEUP_TIME = 2000;
	private static final int REQUEST_MANAGER_FAILURE_THRESHOLD = 3;

	private final SelectorThread selectorThread;
	private final String key;
	private final String multicastGroup;
	private final String sourceIp;
	private final ChannelReceiveInfo channelReceiveInfo;
	private final int maxDroppedPacketsAllowed;
	private final boolean skipGaps;
	private final PriorityQueue<Packet> queuedPackets;

	private long lastSequenceNumber;
	private GapRequestManager requestManager;
	private long timeOfFirstQueuedPacket;
	private long packetsDropped;
	private long packetsLost;
	private boolean retransmissionsDisabled;
	private int requestManagerFailures;

	public ChannelReceiveSequencer(SelectorThread selectorThread, String key, String multicastGroup, String sourceIp, ChannelReceiveInfo channelReceiveInfo,
			int maxDroppedPacketsAllowed, boolean skipGaps)
	{
		this.selectorThread = selectorThread;
		this.key = key;
		this.multicastGroup = multicastGroup;
		this.sourceIp = sourceIp;
		this.channelReceiveInfo = channelReceiveInfo;
		this.maxDroppedPacketsAllowed = maxDroppedPacketsAllowed;
		this.skipGaps = skipGaps;
		this.queuedPackets = new PriorityQueue<Packet>();

		this.lastSequenceNumber = 0;
		this.requestManager = null;
		this.timeOfFirstQueuedPacket = 0;
		this.packetsDropped = 0;
		this.packetsLost = 0;
		this.retransmissionsDisabled = false;
		this.requestManagerFailures = 0;
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
				if (this.requestManager != null)
				{
					this.requestManager.close(false);
					this.requestManager = null;
				}
				this.packetsDropped = 0;
			}
		}
		else
		{
			addPacketToQueue(sequenceNumber, messageCount, packetBuffer);
			PandaErrorCode skipReason = null;
			if (shouldDeclareDrop())
			{
				long dropped = this.queuedPackets.peek().getSequenceNumber() - this.lastSequenceNumber - 1;

				// Check if retransmissions are turned off by receiver
				if (this.skipGaps || this.retransmissionsDisabled)
				{
					skipReason = PandaErrorCode.NONE;
					this.packetsDropped += dropped;
				}
				// Retransmissions are supported
				else if (supportsRetranmissions)
				{
					// Check if max drops exceeded
					if (this.packetsDropped >= this.maxDroppedPacketsAllowed)
					{
						skipReason = PandaErrorCode.PACKET_LOSS_MAX_DROPS_EXCEEDED;
						this.packetsDropped += dropped;
					}
					// Send retransmission request
					else
					{
						if (this.requestManager != null && System.currentTimeMillis() - this.requestManager.getTimeOfRequest() >= QUEUE_GIVEUP_TIME)
						{
							skipReason = PandaErrorCode.PACKET_LOSS_RETRANSMISSION_TIMEOUT;
							this.requestManager.close(false);
						}
						else if (this.requestManagerFailures >= REQUEST_MANAGER_FAILURE_THRESHOLD)
						{
							skipReason = PandaErrorCode.PACKET_LOSS_RETRANSMISSION_FAILED;
							this.requestManagerFailures = 0;
						}
						else
						{
							boolean success = this.sendGapRequest(retransmissionPort);
							skipReason = (success) ? null : PandaErrorCode.PACKET_LOSS_UNABLE_TO_HANDLE_GAP;
							if (this.requestManagerFailures == 0) this.packetsDropped += dropped;
						}
					}
				}
				// Retransmissions not supported
				else
				{
					skipReason = PandaErrorCode.NONE;
					this.packetsDropped += dropped;
				}
			}

			if (skipReason != null)
			{
				if (skipReason != PandaErrorCode.NONE)
				{
					this.channelReceiveInfo.deliverErrorToListeners(skipReason, "Source=" + this.key + " packetsDropped=" + this.packetsDropped, null);
				}
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

	private boolean sendGapRequest(int retransmissionPort)
	{
		if (this.requestManager != null) return true;

		Packet headPacket = this.queuedPackets.peek();
		if (headPacket.getSequenceNumber() > this.lastSequenceNumber + 1)
		{
			this.requestManager = new GapRequestManager(this.selectorThread, this.multicastGroup, this.sourceIp, this);
			int packetCount = (int) (headPacket.getSequenceNumber() - this.lastSequenceNumber - 1);
			return this.requestManager.sendGapRequest(retransmissionPort, this.lastSequenceNumber + 1, packetCount);
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
		if (System.currentTimeMillis() - this.timeOfFirstQueuedPacket >= QUEUE_GIVEUP_TIME) return true;
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

	public void closeRequestManager(boolean successful)
	{
		this.requestManager = null;
		this.requestManagerFailures = (successful) ? 0 : this.requestManagerFailures + 1;
	}

	public void disableRetransmissions()
	{
		this.retransmissionsDisabled = true;
		this.channelReceiveInfo.deliverErrorToListeners(PandaErrorCode.RETRANSMISSION_DISABLED, "Source=" + this.key, null);
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
		return this.requestManager;
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