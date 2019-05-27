package com.ms.silverking.cloud.dht.meta;

import com.ms.silverking.text.FieldsRequirement;
import com.ms.silverking.text.ObjectDefParser2;

public class ConvictionLimits {
	private final int	totalGuiltyServers;
	private final int	guiltyServersPerHour;
	
	private static final ConvictionLimits	template = new ConvictionLimits(0, 0);
	
	static {
        ObjectDefParser2.addParser(template, FieldsRequirement.REQUIRE_ALL_FIELDS);
	}
	
	public ConvictionLimits(int totalGuiltyServers, int guiltyServersPerHour) {
		this.totalGuiltyServers = totalGuiltyServers;
		this.guiltyServersPerHour = guiltyServersPerHour;
	}
	
	public int getTotalGuiltyServers() {
		return totalGuiltyServers;
	}
	
	public int getGuiltyServersPerHour() {
		return guiltyServersPerHour;
	}
	
	@Override
	public boolean equals(Object obj) {
		ConvictionLimits	o;
		
		o = (ConvictionLimits)obj;
		return this.totalGuiltyServers == o.totalGuiltyServers && this.guiltyServersPerHour == o.guiltyServersPerHour;
	}
	
	@Override
	public int hashCode() {
		return Integer.hashCode(totalGuiltyServers) ^ Integer.hashCode(guiltyServersPerHour);
	}
	
	@Override
	public String toString() {
		return ObjectDefParser2.objectToString(this);
	}

	public static ConvictionLimits parse(String def) {
		return ObjectDefParser2.parse(ConvictionLimits.class, def);
	}
}
