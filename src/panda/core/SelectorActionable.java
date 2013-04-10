package panda.core;

interface SelectorActionable
{
	public static final int SEND_MULTICAST = 1;
	public static final int REGISTER_MULTICAST_READ = 2;
	public static final int REGISTER_TCP_ACTION = 3;

	public int getAction();
}
