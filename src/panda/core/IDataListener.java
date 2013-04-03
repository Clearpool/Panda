package panda.core;

import java.nio.ByteBuffer;

public interface IDataListener
{
	public void receivedPandaData(int topicId, ByteBuffer payload);
}