package panda.core;

import java.nio.ByteBuffer;

public interface PandaDataListener
{
	public void receivedPandaData(int topicId, ByteBuffer payload);
	public void receivedPandaError(PandaErrorCode issueCode, String multicastGroup, String message, Throwable throwable);
}