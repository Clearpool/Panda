package com.clearpool.panda.core;

import java.nio.ByteBuffer;

public interface PandaDataListener
{
	void receivedPandaData(String topic, ByteBuffer payload);
	void receivedPandaError(PandaErrorCode issueCode, String multicastGroup, String message, Throwable throwable);
}