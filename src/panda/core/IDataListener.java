package panda.core;

import java.nio.ByteBuffer;

public interface IDataListener
{
	public void receivedFmcData(ByteBuffer payload);
}