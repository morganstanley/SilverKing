package com.ms.silverking.cloud.dht.meta;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class IpAliasingConfigurationReflectionTest {

  @Test
  public void testReflectConfig() {
    String def = "ipAliasMap={192.168.0.1=10.102.102.10:7777,192.168.0.2=10.102.102.11:7777,192.168.0.3=10.102" +
        ".102.10:6614}";
    IpAliasConfiguration parsed = IpAliasConfiguration.parse(def, 0);

    Map<String, String> parsedMap = parsed.getIPAliasMap();
    assert (parsedMap != null);
    assert (!parsedMap.isEmpty());

    Map<String, String> expected = new HashMap<>();
    expected.put("192.168.0.1", "10.102.102.10:7777");
    expected.put("192.168.0.2", "10.102.102.11:7777");
    expected.put("192.168.0.3", "10.102.102.10:6614");

    for (Map.Entry<String, String> e : expected.entrySet()) {
      assert (parsedMap.get(e.getKey()).equals(e.getValue()));
    }
  }

  @Test
  public void testReflectEmptyConfig() {
    String def = "ipAliasMap={}";
    IpAliasConfiguration parsed = IpAliasConfiguration.parse(def, 0);

    Map<String, String> parsedMap = parsed.getIPAliasMap();
    assert (parsedMap != null);
    assert (parsedMap.isEmpty());
  }

}
