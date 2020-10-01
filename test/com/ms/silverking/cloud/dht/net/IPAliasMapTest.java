package com.ms.silverking.cloud.dht.net;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.meta.IpAliasConfiguration;
import com.ms.silverking.net.IPAndPort;
import org.junit.Test;

public class IPAliasMapTest {

  private Map<IPAndPort, IPAndPort[]> createTestMap() {
    Map<IPAndPort, IPAndPort[]> m;

    m = new HashMap<>();
    m.put(new IPAndPort("10.0.0.1:7777"), IPAndPort.parseToArray("192.168.0.1:1000,192.168.0.1:2000"));
    m.put(new IPAndPort("10.0.0.2:7777"), IPAndPort.parseToArray("192.168.0.2:1000"));
    m.put(new IPAndPort("10.0.0.3:7777"), IPAndPort.parseToArray("192.168.0.3:1000"));
    return ImmutableMap.copyOf(m);
  }

  @Test
  public void test() {
    Map<IPAndPort, IPAndPort[]> sourceMap;
    IPAliasMap m;
    IPAndPort addedInterface;
    IPAndPort targetDaemon;

    sourceMap = createTestMap();
    m = new IPAliasMap(sourceMap);
    for (Map.Entry<IPAndPort, IPAndPort[]> mapEntry : sourceMap.entrySet()) {
      IPAndPort daemonIP;
      IPAndPort[] interfaceIPs;
      Set<IPAndPort> interfaceIPSet;

      daemonIP = mapEntry.getKey();
      interfaceIPs = mapEntry.getValue();
      interfaceIPSet = ImmutableSet.copyOf(interfaceIPs);
      assertTrue(interfaceIPSet.contains(m.daemonToInterface(daemonIP)));
      for (IPAndPort interfaceIP : interfaceIPs) {
        assertEquals(m.interfaceToDaemon(interfaceIP), daemonIP);
      }
    }

    addedInterface = new IPAndPort("192.168.0.3:2000");
    targetDaemon = new IPAndPort("10.0.0.3:7777");
    m.addInterfaceToDaemon(addedInterface, targetDaemon);
    assertEquals(m.interfaceToDaemon(addedInterface), targetDaemon);
  }

  @Test
  public void testFromConfiguration() {
    IPAliasMap m;
    String def = "ipAliasMap={192.168.0.1=10.102.102.10:7777,192.168.0.2=10.102.102.11:7777," + "192.168.0.3=10.102" +
        ".102.10:6614}";
    m = new IPAliasMap(IPAliasingUtil.parseConfigAliasMap(IpAliasConfiguration.parse(def, 0).getIPAliasMap(), 7777));

    Map<IPAndPort, IPAndPort> expected = new HashMap<>();
    expected.put(new IPAndPort("192.168.0.1:7777"), new IPAndPort("10.102.102.10:7777"));
    expected.put(new IPAndPort("192.168.0.2:7777"), new IPAndPort("10.102.102.11:7777"));
    expected.put(new IPAndPort("192.168.0.3:7777"), new IPAndPort("10.102.102.10:6614"));

    for (Map.Entry<IPAndPort, IPAndPort> e : expected.entrySet()) {
      assertTrue(m.daemonToInterface(e.getKey()).equals(e.getValue()));
      assertTrue(m.interfaceToDaemon(e.getValue()).equals(e.getKey()));
    }
  }
}
