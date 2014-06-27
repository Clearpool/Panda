package com.clearpool.panda.core;

public class PandaUtils
{
	// Sizes
	static final int PANDA_PACKET_SIZE = 1500;
	static final int MAX_UDP_SIZE = 65535;
	static final int MAX_TCP_SIZE = 65535;
	static final byte PACKET_HEADER_SIZE = 11;
	static final int MESSAGE_HEADER_FIXED_SIZE = 3; // topicId - 1 bytes, message length - 2 bytes
	static final int NETWORK_HEADER_SIZE = 54; // Ethernet header - 26 bytes, IP header - 20 bytes, UDP header - 8 bytes
	static final int PANDA_PACKET_PAYLOAD_SIZE = PANDA_PACKET_SIZE - PACKET_HEADER_SIZE - NETWORK_HEADER_SIZE;
	static final int PANDA_PACKET_MESSAGE_PAYLOAD_SIZE = PANDA_PACKET_PAYLOAD_SIZE - MESSAGE_HEADER_FIXED_SIZE;
	static final int MAX_UDP_PACKET_PAYLOAD_SIZE = MAX_UDP_SIZE - PACKET_HEADER_SIZE - NETWORK_HEADER_SIZE;
	static final int MAX_UDP_MESSAGE_PAYLOAD_SIZE = MAX_UDP_PACKET_PAYLOAD_SIZE - MESSAGE_HEADER_FIXED_SIZE;
	static final int RETRANSMISSION_RESPONSE_HEADER_SIZE = 12;
	static final int RETRANSMISSION_RESPONSE_PACKET_HEADER_SIZE = 4;
	static final int RETRANSMISSION_REQUEST_HEADER_SIZE = 13;
	static final int BLOCKING_QUEUE_SIZE = 1 << 12;

	public static String getMulticastGroup(String ip, int port)
	{
		return ip + ":" + port;
	}
}