package com.clearpool.panda.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

class GapResponseManager
{
	private static final Logger LOGGER = Logger.getLogger(GapResponseManager.class.getName());

	private final SocketChannel channel;
	private final List<byte[]> packets;
	private final int totalPackets;
	private final long startSequenceNumber;

	private ByteBuffer previousBuffer;

	GapResponseManager(SocketChannel channel, List<byte[]> packets, long startSequenceNumber)
	{
		this.channel = channel;
		this.packets = packets;
		this.totalPackets = (packets == null ? 0 : packets.size());
		this.startSequenceNumber = startSequenceNumber;

		this.previousBuffer = null;
	}

	// Called by selectorThread
	void sendResponse(SelectionKey key)
	{
		try
		{
			// Send previous send which was fragmented
			if (this.previousBuffer != null)
			{
				boolean allWritten = writeBufferToChannel(this.previousBuffer);
				if (!allWritten) return;
			}

			// Send Header
			if (this.packets == null || this.packets.size() == this.totalPackets)
			{
				ByteBuffer buffer = ByteBuffer.allocate(PandaUtils.RETRANSMISSION_RESPONSE_HEADER_SIZE);
				buffer.putLong(this.startSequenceNumber);
				buffer.putInt(this.totalPackets);
				buffer.rewind();
				if (!writeBufferToChannel(buffer)) return;
			}

			// Send Response
			if (this.packets != null)
			{
				// Send remaining packets
				Iterator<byte[]> responseIterator = this.packets.iterator();
				while (responseIterator.hasNext())
				{
					byte[] packetBytes = responseIterator.next();
					responseIterator.remove();
					int packetLength = packetBytes.length;
<<<<<<< HEAD
					if (packetLength <= 0)
					{
						LOGGER.severe("[DEBUG] Sending Negative/Zero Packet Length In Gap Response - ReadBuffer Bytes - " + new String(packetBytes));
=======
					if (packetLength <= 0) // TODO - Remove this If{} once the issue is resolved
					{
						LOGGER.severe("Sending Negative/Zero Packet Length In Gap Response - ReadBuffer Bytes - " + new String(packetBytes));
>>>>>>> dac1a1946521bffdeb7503d0dcdfe7edabe0ce74
					}
					ByteBuffer buffer = ByteBuffer.allocate(PandaUtils.RETRANSMISSION_RESPONSE_PACKET_HEADER_SIZE + packetLength);
					buffer.putInt(packetBytes.length);
					buffer.put(packetBytes);
					buffer.rewind();
					if (!writeBufferToChannel(buffer)) return;
				}
			}

			LOGGER.log(Level.FINER, "Sent GapResponse to " + this.channel.getLocalAddress() + " for " + this.totalPackets + " packets starting with sequenceNumber "
					+ this.startSequenceNumber);

			// Close channel when all responses have been written or if no responses ever existed
			if (this.packets == null || this.packets.size() == 0)
			{
				this.channel.close();
				key.cancel();
			}
		}
		catch (Exception e)
		{
			try
			{
				this.channel.close();
			}
			catch (IOException e1)
			{
			}
			key.cancel();
		}
	}

	private boolean writeBufferToChannel(ByteBuffer buffer) throws IOException
	{
		this.channel.write(buffer);
		if (buffer.remaining() == 0)
		{
			this.previousBuffer = null;
			return true;
		}
		this.previousBuffer = buffer;
		return false;
	}
}