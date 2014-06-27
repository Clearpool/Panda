package com.clearpool.panda.core;

interface SelectorActionable
{
	static final int SEND_MULTICAST = 1;
	static final int REGISTER_MULTICAST_READ = 2;
	static final int REGISTER_TCP_ACTION = 3;

	int getAction();
}
