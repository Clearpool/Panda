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
		TopicInfo topicInfo = new TopicInfo("239.9.9.9", Integer.valueOf(9001), Integer.valueOf(topicID), "TEST_TOPIC");

		ReceiverTester rt = new ReceiverTester();
		rt.SubscribeToTest(0, topicInfo);
	}

}
