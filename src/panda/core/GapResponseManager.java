package panda.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.List;

import panda.utils.Utils;


public class GapResponseManager
{
	private final SocketChannel channel;
	private final List<byte[]> packets;
	private final int totalPackets;
	private final long startSequenceNumber;
	
	private ByteBuffer previousBuffer;
	
	public GapResponseManager(SocketChannel channel, List<byte[]> packets, long startSequenceNumber)
	{
		this.channel = channel;
		this.packets = packets;
		this.totalPackets = (packets == null ? 0 : packets.size());
		this.startSequenceNumber = startSequenceNumber;
		
		this.previousBuffer = null;
	}

	//Called by selectorThread
	public void sendResponse(SelectionKey key)
	{
		try
		{	
			//Send previous send which was fragmented
			if(this.previousBuffer != null)
			{
				boolean allWritten = writeBufferToChannel(this.previousBuffer);
				if(!allWritten) return;
			}
			
			//Send Header
			if(this.packets == null || this.packets.size() == this.totalPackets)
			{
				byte[] bytes = new byte[Utils.RETRANSMISSION_RESPONSE_HEADER_SIZE];
				ByteBuffer buffer = ByteBuffer.wrap(bytes);
				buffer.putLong(this.startSequenceNumber);
				buffer.putInt(this.totalPackets);
				buffer.rewind();
				boolean allWritten = writeBufferToChannel(buffer);
				if(!allWritten) return;
			}
			
			//Send Response
			if(this.packets != null)
			{	
				//Send remaining packets
				Iterator<byte[]> responseIterator = this.packets.iterator();
				while(responseIterator.hasNext())
				{
					byte[] packetBytes = responseIterator.next();
					responseIterator.remove();
					byte[] bytes = new byte[Utils.RETRANSMISSION_RESPONSE_PACKET_HEADER_SIZE + packetBytes.length];
					ByteBuffer buffer = ByteBuffer.wrap(bytes);
					buffer.putInt(packetBytes.length);
					buffer.put(packetBytes);
					buffer.rewind();
					boolean allWritten = writeBufferToChannel(buffer);
					if(!allWritten) return;
				}
			}
			
			//Close channel when all responses have been written or if no responses ever existed
			if(this.packets == null || this.packets.size() == 0)
			{
				this.channel.close();
				key.cancel();
			}
		}
		catch (Exception e)
		{
			try{this.channel.close();} catch (IOException e1){}
			key.cancel();
		}
	}

	private boolean writeBufferToChannel(ByteBuffer buffer) throws IOException
	{
		this.channel.write(buffer);
		if(buffer.remaining() == 0)
		{
			this.previousBuffer = null;
			return true;
		}
		this.previousBuffer = buffer;
		return false;
	}
}