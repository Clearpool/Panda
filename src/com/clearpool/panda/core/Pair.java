package com.clearpool.panda.core;


public class Pair<A, B>
{
	private final A a;
	private final B b;

	public Pair(A a, B b)
	{
		this.a = a;
		this.b = b;
	}

	public A getA()
	{
		return this.a;
	}

	public B getB()
	{
		return this.b;
	}

	@Override
	public boolean equals(Object o)
	{
		if (o == null)
		{
			return false;
		}
		if (!(o instanceof Pair))
		{
			return false;
		}
		Pair<?, ?> p = (Pair<?, ?>) o;
		return (this.a == p.a || (this.a != null && this.a.equals(p.a))) && (this.b == p.b || (this.b != null && this.b.equals(p.b)));
	}

	@Override
	public int hashCode()
	{
		return (this.a == null ? 0 : this.a.hashCode()) ^ (this.b == null ? 0 : this.b.hashCode());
	}

}