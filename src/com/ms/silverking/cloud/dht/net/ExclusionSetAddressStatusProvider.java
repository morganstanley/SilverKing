package com.ms.silverking.cloud.dht.net;

import java.net.InetSocketAddress;

import com.ms.silverking.cloud.meta.ExclusionSet;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.net.async.AddressStatusProvider;

public class ExclusionSetAddressStatusProvider implements AddressStatusProvider {
  private final String addressStatusProviderThreadName;
  private final IPAliasMap  aliasMap;
  private volatile ExclusionSet exclusionSet;

  public ExclusionSetAddressStatusProvider(String addressStatusProviderThreadName, IPAliasMap aliasMap) {
    this.addressStatusProviderThreadName = addressStatusProviderThreadName;
    this.aliasMap = aliasMap;
    exclusionSet = ExclusionSet.emptyExclusionSet(0);
  }

  public void setExclusionSet(ExclusionSet exclusionSet) {
    this.exclusionSet = exclusionSet;
  }

  @Override
  public boolean isHealthy(InetSocketAddress addr) {
    IPAndPort peer;

    peer = aliasMap.interfaceToDaemon(addr);
    return !exclusionSet.contains(peer.getIPAsString());
  }

  @Override
  public boolean isAddressStatusProviderThread(String context) {
    return addressStatusProviderThreadName.equals(context != null ? context : Thread.currentThread().getName());
  }

  @Override
  public boolean isAddressStatusProviderThread() {
    return isAddressStatusProviderThread(Thread.currentThread().getName());
  }
}
