package com.clearpool.panda.core;

public class MutableInteger
{
	private int value;

	public MutableInteger()
	{
		this(0);
	}

	public MutableInteger(int value)
	{
		this.value = value;
	}

	public int getValue()
	{
		return this.value;
	}

	public void setValue(int value)
	{
		this.value = value;
	}

	public int incrementAndGet()
	{
		return ++this.value;
	}

	public int incrementAndGet(long increment)
	{
		return this.value += increment;
	}

	@Override
	public String toString()
	{
		return String.valueOf(this.value);
	}
}
