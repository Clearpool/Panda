package com.clearpool.panda.core;

import java.util.LinkedList;
import java.util.List;

public class PandaReceiveMgmtMBean
{
	public static String[] getPacketsDropped()
	{
		List<String> packetsDropped = new LinkedList<>();
		for (PandaAdapter adapter : PandaAdapter.ALL_PANDA_ADAPTERS.values())
		{
			Receiver receiver = adapter.getReceiver();
			for (ChannelReceiveInfo receiveInfo : receiver.getChannelReceiveInfos().values())
			{
				for (ChannelReceiveSequencer sequencer : receiveInfo.getSourceInfos().values())
				{
					if (sequencer.getPacketsDropped() > 0)
					{
						packetsDropped.add("Source=" + sequencer.getKey() + " + Group=" + receiveInfo.getMulticastGroup() + " - " + sequencer.getPacketsDropped());
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
		for (PandaAdapter adapter : PandaAdapter.ALL_PANDA_ADAPTERS.values())
		{
			Receiver receiver = adapter.getReceiver();
			for (ChannelReceiveInfo receiveInfo : receiver.getChannelReceiveInfos().values())
			{
				for (ChannelReceiveSequencer sequencer : receiveInfo.getSourceInfos().values())
				{
					if (sequencer.getPacketsLost() > 0)
					{
						packetsLost.add("Source=" + sequencer.getKey() + " + Group=" + receiveInfo.getMulticastGroup() + " - " + sequencer.getPacketsLost());
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
		for (PandaAdapter adapter : PandaAdapter.ALL_PANDA_ADAPTERS.values())
		{
			Receiver receiver = adapter.getReceiver();
			for (ChannelReceiveInfo receiveInfo : receiver.getChannelReceiveInfos().values())
			{
				for (ChannelReceiveSequencer sequencer : receiveInfo.getSourceInfos().values())
				{
					queueSize.add("Source=" + sequencer.getKey() + " + Group=" + receiveInfo.getMulticastGroup() + " - " + sequencer.getQueueSize());
				}
			}
		}
		String[] ret = new String[queueSize.size()];
		return queueSize.toArray(ret);
	}

	public static String[] getLastSequenceNumbers()
	{
		List<String> lastSequenceNumbers = new LinkedList<>();
		for (PandaAdapter adapter : PandaAdapter.ALL_PANDA_ADAPTERS.values())
		{
			Receiver receiver = adapter.getReceiver();
			for (ChannelReceiveInfo receiveInfo : receiver.getChannelReceiveInfos().values())
			{
				for (ChannelReceiveSequencer sequencer : receiveInfo.getSourceInfos().values())
				{
					lastSequenceNumbers.add("Source=" + sequencer.getKey() + " + Group=" + receiveInfo.getMulticastGroup() + " - " + sequencer.getLastSequenceNumber());
				}
			}
		}
		String[] ret = new String[lastSequenceNumbers.size()];
		return lastSequenceNumbers.toArray(ret);
	}

	public static String[] getRetransmissionFailures()
	{
		List<String> retransmissionFailures = new LinkedList<>();
		for (PandaAdapter adapter : PandaAdapter.ALL_PANDA_ADAPTERS.values())
		{
			Receiver receiver = adapter.getReceiver();
			for (ChannelReceiveInfo receiveInfo : receiver.getChannelReceiveInfos().values())
			{
				for (ChannelReceiveSequencer sequencer : receiveInfo.getSourceInfos().values())
				{
					if (sequencer.getRetransmissionFailures() > 0)
					{
						retransmissionFailures.add("Source=" + sequencer.getKey() + " + Group=" + receiveInfo.getMulticastGroup() + " - " + sequencer.getRetransmissionFailures());
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
		for (PandaAdapter adapter : PandaAdapter.ALL_PANDA_ADAPTERS.values())
		{
			Receiver receiver = adapter.getReceiver();
			for (ChannelReceiveInfo receiveInfo : receiver.getChannelReceiveInfos().values())
			{
				for (ChannelReceiveSequencer sequencer : receiveInfo.getSourceInfos().values())
				{
					if (sequencer.getNumOfRetransmissionRequests() > 0)
					{
						retransRequestNum.add("Source=" + sequencer.getKey() + " + Group=" + receiveInfo.getMulticastGroup() + " - " + sequencer.getNumOfRetransmissionRequests());
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
			for (PandaAdapter adapter : PandaAdapter.ALL_PANDA_ADAPTERS.values())
			{
				Receiver receiver = adapter.getReceiver();
				for (ChannelReceiveInfo receiveInfo : receiver.getChannelReceiveInfos().values())
				{
					for (ChannelReceiveSequencer sequencer : receiveInfo.getSourceInfos().values())
					{
						int giveUps = sequencer.getRequestGiveUps(errCode);
						if (giveUps > 0)
						{
							requestGiveUps.add("Source=" + sequencer.getKey() + " + Group=" + receiveInfo.getMulticastGroup() + " - " + errCode.name() + "=" + giveUps);
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
		for (PandaAdapter adapter : PandaAdapter.ALL_PANDA_ADAPTERS.values())
		{
			Receiver receiver = adapter.getReceiver();
			for (ChannelReceiveInfo receiveInfo : receiver.getChannelReceiveInfos().values())
			{
				for (ChannelReceiveSequencer sequencer : receiveInfo.getSourceInfos().values())
				{
					if (sequencer.getRetransmissionsDisabled())
					{
						retransDisabled.add("Source=" + sequencer.getKey() + " + Group=" + receiveInfo.getMulticastGroup());
					}
				}
			}
		}
		String[] ret = new String[retransDisabled.size()];
		return retransDisabled.toArray(ret);
	}

	public static void setRetransmissionsDisabled(String multicastGroup, String source, boolean retransmissionsDisabled)
	{
		if (multicastGroup == null || multicastGroup.isEmpty()) return;
		if (source == null || source.isEmpty()) return;

		for (PandaAdapter adapter : PandaAdapter.ALL_PANDA_ADAPTERS.values())
		{
			Receiver receiver = adapter.getReceiver();
			ChannelReceiveInfo receiveInfo = receiver.getChannelReceiveInfos().get(multicastGroup);
			if (receiveInfo != null)
			{
				ChannelReceiveSequencer sequencer = receiveInfo.getSourceInfos().get(source);
				if (sequencer != null)
				{
					sequencer.setRetransmissionsDisabled(retransmissionsDisabled);
				}
			}
		}
	}

	public static void setRetransmissionsDisabledForAllSequencers()
	{
		for (PandaAdapter adapter : PandaAdapter.ALL_PANDA_ADAPTERS.values())
		{
			Receiver receiver = adapter.getReceiver();
			for (ChannelReceiveInfo receiveInfo : receiver.getChannelReceiveInfos().values())
			{
				for (ChannelReceiveSequencer sequencer : receiveInfo.getSourceInfos().values())
				{
					if (sequencer.getRetransmissionsDisabled())
					{
						sequencer.setRetransmissionsDisabled(false);
					}
				}
			}
		}
	}

	public static void resetPacketsDropped(String multicastGroup, String source)
	{
		if (multicastGroup == null || multicastGroup.isEmpty()) return;
		if (source == null || source.isEmpty()) return;

		for (PandaAdapter adapter : PandaAdapter.ALL_PANDA_ADAPTERS.values())
		{
			Receiver receiver = adapter.getReceiver();
			ChannelReceiveInfo receiveInfo = receiver.getChannelReceiveInfos().get(multicastGroup);
			if (receiveInfo != null)
			{
				ChannelReceiveSequencer sequencer = receiveInfo.getSourceInfos().get(source);
				if (sequencer != null)
				{
					sequencer.resetPacketsDropped();
				}
			}
		}
	}

	public static void resetPacketsDroppedIfExceededLimit()
	{
		for (PandaAdapter adapter : PandaAdapter.ALL_PANDA_ADAPTERS.values())
		{
			Receiver receiver = adapter.getReceiver();
			for (ChannelReceiveInfo receiveInfo : receiver.getChannelReceiveInfos().values())
			{
				for (ChannelReceiveSequencer sequencer : receiveInfo.getSourceInfos().values())
				{
					if (sequencer.getPacketsDropped() >= sequencer.getMaxDroppedPacketsAllowed())
					{
						sequencer.resetPacketsDropped();
					}
				}
			}
		}
	}
}
