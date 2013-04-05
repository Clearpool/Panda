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
		if (args.length != 4)
		{
			System.out.println(" usage -- panda.core.RecvTest2 (int)pandaAdapterCache (int)topicID (long)numOfMessages (int)netRecvBufferSize");
			System.exit(0);
		}
		int adapterCache = Integer.valueOf(args[0]).intValue();
		int topicID = Integer.valueOf(args[1]).intValue();
		long numOfMessages = Long.valueOf(args[2]).longValue();
		int netRecvBufferSize = Integer.valueOf(args[3]).intValue();
		TopicInfo topicInfo = new TopicInfo("239.9.9.9", Integer.valueOf(9001), Integer.valueOf(topicID), "TEST_TOPIC");

		ReceiverTester.subscribeToSequencedMessages(adapterCache, topicInfo, numOfMessages, netRecvBufferSize);
	}
}