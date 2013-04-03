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
		int topicID = Integer.valueOf(args[0]).intValue();
		int payloadSize = Integer.valueOf(args[1]).intValue();

		int numOfMessages = Integer.valueOf(args[2]).intValue();
		final TopicInfo topicInfo = new TopicInfo("239.9.9.9", Integer.valueOf(9001), Integer.valueOf(topicID), "TEST_TOPIC");

		SenderTester st = new SenderTester();
		// st.SendToTest(10000, topicInfo,
		// /*Utils.MAX_MESSAGE_PAYLOAD_SIZE*/payloadSize, intervalNanoSec);
		st.SendSequencedMessages(10000, topicInfo, payloadSize, numOfMessages);
		System.exit(0);
	}
}
