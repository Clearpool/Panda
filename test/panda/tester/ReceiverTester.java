package panda.tester;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;

import panda.core.PandaAdapter;
import panda.core.IDataListener;
import panda.core.containers.TopicInfo;

public class ReceiverTester
{
	public ReceiverTester()
	{

	}

	public void SubscribeToTest(int cacheSize, TopicInfo tInfo) throws Exception
	{
		final PandaAdapter adapter = new PandaAdapter(cacheSize);
		final TopicInfo topicInfo = tInfo;
		adapter.subscribe(topicInfo, InetAddress.getLocalHost().getHostAddress(), new IDataListener() {
			long lastRecdSenderTime = 0;
			long lastRecdSenderSeq = 0;
			long packetPerSecCounter = 0;
			@Override
			public void receivedPandaData(int topicId, ByteBuffer payload)
			{
				long recdSenderTime = payload.getLong();
				long recdSenderSeq = payload.getLong();

				if (recdSenderTime > this.lastRecdSenderTime)
				{
					//if(this.lastRecdSenderTime != 0) System.out.println(" Packets Recd in sender second " + new Date(this.lastRecdSenderTime) + " = " + this.packetPerSecCounter + " and dropped " + (this.lastRecdSenderSeq - this.packetPerSecCounter) + " packets.");
					this.packetPerSecCounter = 0;
					this.lastRecdSenderTime = recdSenderTime;
					this.lastRecdSenderSeq = 0;
				}
				else if (recdSenderSeq != (this.lastRecdSenderSeq + 1))
				{
					//System.out.println(new Date() + " Lost packets between " + this.lastRecdSenderSeq + " and " + recdSenderSeq);
				}
				this.lastRecdSenderSeq = recdSenderSeq;
				++this.packetPerSecCounter;
				//System.out.println(new Date() + " Recd Packet Sender time = " + recdSenderTime + " Recd Packet Seq = " + recdSenderSeq);
			}
		}, 10000000);
	}
}
