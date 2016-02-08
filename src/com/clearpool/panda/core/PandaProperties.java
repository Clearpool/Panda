package com.clearpool.panda.core;

import java.util.HashMap;
import java.util.Map;

public class PandaProperties
{
	public static final String PEG_SELECTOR_START_TIME = "PEG_SELECTOR_START_TIME";
	public static final String PEG_SELECTOR_END_TIME = "PEG_SELECTOR_END_TIME";
	public static final String MAINTAIN_DETAILED_STATS = "MAINTAIN_DETAILED_STATS";

	private final Map<String, String> props;

	public PandaProperties()
	{
		this.props = new HashMap<String, String>();
	}

	public PandaProperties(PandaProperties otherProperties)
	{
		this.props = new HashMap<String, String>();
		if (otherProperties != null)
		{
			this.props.putAll(otherProperties.props);
		}
	}

	public void setProperty(String property, String value)
	{
		this.props.put(property, value);
	}

	public long getLongProperty(String property, long defaultValue)
	{
		String value = this.props.get(property);
		if (value == null) return defaultValue;
		return Long.valueOf(value).longValue();
	}

	public boolean getBooleanProperty(String property, boolean defaultValue)
	{
		String value = this.props.get(property);
		if (value == null) return defaultValue;
		return Boolean.valueOf(value).booleanValue();
	}
}
