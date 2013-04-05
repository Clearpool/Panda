package panda.core;

import panda.core.containers.TopicInfo;
import panda.tester.SenderTester;

public class SenderTest2
{

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception
	{
		if (args.length != 5)
		{
			System.out.println(" usage -- panda.core.SenderTest2 (int)pandaAdapterCache (int)topicID (int)numOfMessages (int)payloadSize (int)netRecvBufferSize");
			System.exit(0);
		}
		int adapterCache = Integer.valueOf(args[0]).intValue();
		int topicID = Integer.valueOf(args[1]).intValue();
		int numOfMessages = Integer.valueOf(args[2]).intValue();
		int payloadSize = Integer.valueOf(args[3]).intValue();
		int netRecvBufferSize = Integer.valueOf(args[4]).intValue();

		
		final TopicInfo topicInfo = new TopicInfo("239.9.9.9", Integer.valueOf(9001), Integer.valueOf(topicID), "TEST_TOPIC");

		SenderTester st = new SenderTester();
		// st.SendToTest(10000, topicInfo,
		// /*Utils.MAX_MESSAGE_PAYLOAD_SIZE*/payloadSize, intervalNanoSec);
		st.SendSequencedMessages(adapterCache, topicInfo, payloadSize, numOfMessages, netRecvBufferSize);
		System.exit(0);
	}
}
