package com.ms.silverking.cloud.meta;

import java.net.InetSocketAddress;

import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.net.async.AddressStatusProvider;

public class ExclusionSetAddressStatusProvider implements AddressStatusProvider {
	private final String	addressStatusProviderThreadName;
	private volatile ExclusionSet	exclusionSet;
	
	public ExclusionSetAddressStatusProvider(String addressStatusProviderThreadName) {
		this.addressStatusProviderThreadName = addressStatusProviderThreadName;
		exclusionSet = ExclusionSet.emptyExclusionSet(0);
	}
	
	public void setExclusionSet(ExclusionSet exclusionSet) {
		this.exclusionSet = exclusionSet;
	}
	
	@Override
	public boolean isHealthy(InetSocketAddress addr) {
		return !exclusionSet.contains(new IPAndPort(addr).getIPAsString());
	}

	@Override
	public boolean isAddressStatusProviderThread() {
		return Thread.currentThread().getName().equals(addressStatusProviderThreadName);
	}
}
