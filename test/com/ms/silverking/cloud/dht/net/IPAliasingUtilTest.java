package com.ms.silverking.cloud.dht.net;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.ms.silverking.net.IPAndPort;
import org.junit.Test;

public class IPAliasingUtilTest {

  private int fixedPort = 7777;

  @Test
  public void testAliasing() {
    Map<String, String> aliasMap = new HashMap<>();
    aliasMap.put("10.100.0.1", "192.168.0.1:9999");
    aliasMap.put("10.100.0.2", "192.168.0.2");

    Map<IPAndPort, IPAndPort[]> resolved = IPAliasingUtil.parseConfigAliasMap(aliasMap, fixedPort);
    assert (resolved != null);
    assert (!resolved.isEmpty());

    for (Map.Entry<String, String> e : aliasMap.entrySet()) {
      IPAndPort key = new IPAndPort(e.getKey(), fixedPort);
      IPAndPort[] value;
      if (e.getValue().contains(":")) {
        String[] tokens = e.getValue().split(":");
        value = new IPAndPort[] { new IPAndPort(tokens[0], tokens[1]) };
      } else {
        value = new IPAndPort[] { new IPAndPort(e.getValue(), fixedPort) };
      }

      assert (resolved.containsKey(key));
      assert (Arrays.equals(resolved.get(key), value));
    }
  }

  @Test
  public void testAliasingOfEmptyMap() {
    Map<String, String> aliasMap = new HashMap<>();
    Map<IPAndPort, IPAndPort[]> resolved = IPAliasingUtil.parseConfigAliasMap(aliasMap, fixedPort);
    assert (resolved != null);
    assert (resolved.isEmpty());
  }
}
