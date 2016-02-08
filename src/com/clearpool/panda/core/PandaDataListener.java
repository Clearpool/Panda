package com.clearpool.panda.core;


public interface PandaDataListener
{
	void receivedPandaData(String topic, byte[] payload);

	void receivedPandaError(PandaErrorCode issueCode, String multicastGroup, String message, Throwable throwable);
}