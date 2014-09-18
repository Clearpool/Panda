package com.clearpool.panda.core;

import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

public class PandaReceiveMgmtMBean
{
	public static String[] getPacketsDropped()
	{
		List<String> packetsDropped = new LinkedList<>();
		for (PandaAdapter adapter : PandaAdapter.ALL_PANDA_ADAPTERS)
		{
			Receiver receiver = adapter.getReceiver();
			for (ChannelReceiveInfo receiveInfo : receiver.getChannelReceiveInfos().values())
			{
				for (Entry<String, ChannelReceiveSequencer> sourceInfo : receiveInfo.getSourceInfos().entrySet())
				{
					ChannelReceiveSequencer sequencer = sourceInfo.getValue();
					if (sequencer.getPacketsDropped() > 0)
					{
						String[] source = sourceInfo.getKey().split("@");
						packetsDropped.add("Packets Dropped " + sequencer.getPacketsDropped() + " from " + source[1] + ":" + source[0] + " - Multicast Group = "
								+ receiveInfo.getMulticastGroup());
					}
				}
			}
		}
		String[] ret = new String[packetsDropped.size()];
		return packetsDropped.toArray(ret);
	}

	public static String[] getPacketsLost()
	{
		List<String> packetsLost = new LinkedList<>();
		for (PandaAdapter adapter : PandaAdapter.ALL_PANDA_ADAPTERS)
		{
			Receiver receiver = adapter.getReceiver();
			for (ChannelReceiveInfo receiveInfo : receiver.getChannelReceiveInfos().values())
			{
				for (Entry<String, ChannelReceiveSequencer> sourceInfo : receiveInfo.getSourceInfos().entrySet())
				{
					ChannelReceiveSequencer sequencer = sourceInfo.getValue();
					if (sequencer.getPacketsLost() > 0)
					{
						String[] source = sourceInfo.getKey().split("@");
						packetsLost.add("Packets Lost " + sequencer.getPacketsLost() + " from " + source[1] + ":" + source[0] + " - Multicast Group = "
								+ receiveInfo.getMulticastGroup());
					}
				}
			}
		}
		String[] ret = new String[packetsLost.size()];
		return packetsLost.toArray(ret);
	}

	public static String[] getQueueSize()
	{
		List<String> queueSize = new LinkedList<>();
		for (PandaAdapter adapter : PandaAdapter.ALL_PANDA_ADAPTERS)
		{
			Receiver receiver = adapter.getReceiver();
			for (ChannelReceiveInfo receiveInfo : receiver.getChannelReceiveInfos().values())
			{
				for (Entry<String, ChannelReceiveSequencer> sourceInfo : receiveInfo.getSourceInfos().entrySet())
				{
					ChannelReceiveSequencer sequencer = sourceInfo.getValue();
					String[] source = sourceInfo.getKey().split("@");
					queueSize.add("Receive Queue Size " + sequencer.getQueueSize() + " for " + source[1] + ":" + source[0] + " - Multicast Group = "
							+ receiveInfo.getMulticastGroup());
				}
			}
		}
		String[] ret = new String[queueSize.size()];
		return queueSize.toArray(ret);
	}

	public static String[] getRetransmissionFailures()
	{
		List<String> retransmissionFailures = new LinkedList<>();
		for (PandaAdapter adapter : PandaAdapter.ALL_PANDA_ADAPTERS)
		{
			Receiver receiver = adapter.getReceiver();
			for (ChannelReceiveInfo receiveInfo : receiver.getChannelReceiveInfos().values())
			{
				for (Entry<String, ChannelReceiveSequencer> sourceInfo : receiveInfo.getSourceInfos().entrySet())
				{
					ChannelReceiveSequencer sequencer = sourceInfo.getValue();
					if (sequencer.getRetransmissionFailures() > 0)
					{
						String[] source = sourceInfo.getKey().split("@");
						retransmissionFailures.add("Retransmission Failures " + sequencer.getRetransmissionFailures() + " to " + source[1] + ":" + source[0]
								+ " - Multicast Group = " + receiveInfo.getMulticastGroup());
					}
				}
			}
		}
		String[] ret = new String[retransmissionFailures.size()];
		return retransmissionFailures.toArray(ret);
	}

	public static String[] getNumOfRetransmissionRequests()
	{
		List<String> retransRequestNum = new LinkedList<>();
		for (PandaAdapter adapter : PandaAdapter.ALL_PANDA_ADAPTERS)
		{
			Receiver receiver = adapter.getReceiver();
			for (ChannelReceiveInfo receiveInfo : receiver.getChannelReceiveInfos().values())
			{
				for (Entry<String, ChannelReceiveSequencer> sourceInfo : receiveInfo.getSourceInfos().entrySet())
				{
					ChannelReceiveSequencer sequencer = sourceInfo.getValue();
					if (sequencer.getNumOfRetransmissionRequests() > 0)
					{
						String[] source = sourceInfo.getKey().split("@");
						retransRequestNum.add("Retransmission Requests " + sequencer.getNumOfRetransmissionRequests() + " to " + source[1] + ":" + source[0]
								+ " - Multicast Group = " + receiveInfo.getMulticastGroup());
					}
				}
			}
		}
		String[] ret = new String[retransRequestNum.size()];
		return retransRequestNum.toArray(ret);
	}

	public static String[] getRequestGiveUpsByErrorCode()
	{
		List<String> requestGiveUps = new LinkedList<>();
		for (PandaErrorCode errCode : PandaErrorCode.values())
		{
			for (PandaAdapter adapter : PandaAdapter.ALL_PANDA_ADAPTERS)
			{
				Receiver receiver = adapter.getReceiver();
				for (ChannelReceiveInfo receiveInfo : receiver.getChannelReceiveInfos().values())
				{
					for (Entry<String, ChannelReceiveSequencer> sourceInfo : receiveInfo.getSourceInfos().entrySet())
					{
						ChannelReceiveSequencer sequencer = sourceInfo.getValue();
						int giveUps = sequencer.getRequestGiveUps(errCode);
						if (giveUps > 0)
						{
							String[] source = sourceInfo.getKey().split("@");
							requestGiveUps.add("Retransmission Attempt Giveups due to PandaErrorCode " + errCode.name() + " - " + giveUps + " to " + source[1] + ":" + source[0]
									+ " - Multicast Group = " + receiveInfo.getMulticastGroup());
						}
					}
				}
			}
		}
		String[] ret = new String[requestGiveUps.size()];
		return requestGiveUps.toArray(ret);
	}

	public static String[] getRetransmissionsDisabledfConnections()
	{
		List<String> retransDisabled = new LinkedList<>();
		for (PandaAdapter adapter : PandaAdapter.ALL_PANDA_ADAPTERS)
		{
			Receiver receiver = adapter.getReceiver();
			for (ChannelReceiveInfo receiveInfo : receiver.getChannelReceiveInfos().values())
			{
				for (Entry<String, ChannelReceiveSequencer> sourceInfo : receiveInfo.getSourceInfos().entrySet())
				{
					ChannelReceiveSequencer sequencer = sourceInfo.getValue();
					if (sequencer.getRetransmissionsDisabled())
					{
						String[] source = sourceInfo.getKey().split("@");
						retransDisabled.add("Retransmission disabled to " + source[1] + ":" + source[0] + " - Multicast Group = " + receiveInfo.getMulticastGroup());
					}
				}
			}
		}
		String[] ret = new String[retransDisabled.size()];
		return retransDisabled.toArray(ret);
	}

	public static void setRetransmissionsDisabled(String multicastGroup, String sourceIp, int port, boolean retransmissionsDisabled)
	{
		if (multicastGroup == null || multicastGroup.isEmpty()) return;
		if (sourceIp == null || sourceIp.isEmpty()) return;

		for (PandaAdapter adapter : PandaAdapter.ALL_PANDA_ADAPTERS)
		{
			Receiver receiver = adapter.getReceiver();
			ChannelReceiveInfo receiveInfo = receiver.getChannelReceiveInfos().get(multicastGroup);
			if (receiveInfo != null)
			{
				ChannelReceiveSequencer sequencer = receiveInfo.getSourceInfos().get(port + "@" + sourceIp);
				if (sequencer != null)
				{
					sequencer.setRetransmissionsDisabled(retransmissionsDisabled);
				}
			}
		}
	}

	public static void setRetransmissionsDisabledForAllSequencers()
	{
		for (PandaAdapter adapter : PandaAdapter.ALL_PANDA_ADAPTERS)
		{
			Receiver receiver = adapter.getReceiver();
			for (ChannelReceiveInfo receiveInfo : receiver.getChannelReceiveInfos().values())
			{
				for (Entry<String, ChannelReceiveSequencer> sourceInfo : receiveInfo.getSourceInfos().entrySet())
				{
					ChannelReceiveSequencer sequencer = sourceInfo.getValue();
					if (sequencer.getRetransmissionsDisabled())
					{
						sequencer.setRetransmissionsDisabled(false);
					}
				}
			}
		}
	}

	public static void resetPacketsDropped(String multicastGroup, String sourceIp, int port)
	{
		if (multicastGroup == null || multicastGroup.isEmpty()) return;
		if (sourceIp == null || sourceIp.isEmpty()) return;

		for (PandaAdapter adapter : PandaAdapter.ALL_PANDA_ADAPTERS)
		{
			Receiver receiver = adapter.getReceiver();
			ChannelReceiveInfo receiveInfo = receiver.getChannelReceiveInfos().get(multicastGroup);
			if (receiveInfo != null)
			{
				ChannelReceiveSequencer sequencer = receiveInfo.getSourceInfos().get(port + "@" + sourceIp);
				if (sequencer != null)
				{
					sequencer.resetPacketsDropped();
				}
			}
		}
	}

	public static void resetPacketsDroppedIfExceededLimit()
	{
		for (PandaAdapter adapter : PandaAdapter.ALL_PANDA_ADAPTERS)
		{
			Receiver receiver = adapter.getReceiver();
			for (ChannelReceiveInfo receiveInfo : receiver.getChannelReceiveInfos().values())
			{
				for (Entry<String, ChannelReceiveSequencer> sourceInfo : receiveInfo.getSourceInfos().entrySet())
				{
					ChannelReceiveSequencer sequencer = sourceInfo.getValue();
					if (sequencer.getPacketsDropped() >= sequencer.getMaxDroppedPacketsAllowed())
					{
						sequencer.resetPacketsDropped();
					}
				}
			}
		}
	}
}
