package panda.tester;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;

import panda.core.PandaAdapter;
import panda.core.containers.TopicInfo;
import panda.utils.Utils;

public class SenderTester
{
	public SenderTester()
	{

	}

	// 16 <= dataSize <= Utils.MAX_MESSAGE_PAYLOAD_SIZE
	public void SendToTest(int cacheSize, TopicInfo tInfo, int dataSize, int timeBetweenPacketsNanoSec) throws Exception
	{
		final PandaAdapter adapter = new PandaAdapter(cacheSize);
		final TopicInfo topicInfo = tInfo;
		long lastSenderSeq = 0;
		long lastSenderTime = 0;
		// long bytesSent = 0;

		while (true)
		{
			long currTimeSecs = (System.currentTimeMillis() / 1000);
			if (lastSenderTime < 1000 * currTimeSecs)
			{
				if(lastSenderTime != 0) System.out.println("Packets sent in the second " + new Date(lastSenderTime) + " = " + lastSenderSeq);
				lastSenderTime = 1000 * currTimeSecs;
				lastSenderSeq = 0;
				// System.out.println(new Date() + " Bytes Sent So Far= " +
				// bytesSent);
			}
			ByteBuffer buffer = ByteBuffer.allocate(dataSize);
			buffer.putLong(0, lastSenderTime);
			buffer.putLong(8, ++lastSenderSeq);

			adapter.send(topicInfo, InetAddress.getLocalHost().getHostAddress(), buffer.array());
			
		}
	}
}
