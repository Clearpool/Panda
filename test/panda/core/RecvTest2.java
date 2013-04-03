package panda.core;

import panda.core.containers.TopicInfo;
import panda.tester.ReceiverTester;

public class RecvTest2
{

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception
	{
		short topicID = Short.valueOf(args[0]).shortValue();
		long numOfMessages = Long.valueOf(args[1]).longValue();
		TopicInfo topicInfo = new TopicInfo("239.9.9.9", Integer.valueOf(9001), Integer.valueOf(topicID), "TEST_TOPIC");


		ReceiverTester rt = new ReceiverTester();
		rt.SubscribeToSequencedMessages(10000, topicInfo, numOfMessages, 10000000);
	}

}
