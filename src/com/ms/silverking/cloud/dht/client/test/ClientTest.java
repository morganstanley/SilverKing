package com.ms.silverking.cloud.dht.client.test;

import java.util.List;

import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.Namespace;
import com.ms.silverking.collection.Pair;

public interface ClientTest {
  public String getTestName();

  public List<NamespaceOptions> getNamespaceOptions(NamespaceOptions sessionDefaultNSOptions);

  public Pair<Integer, Integer> runTest(DHTSession session, Namespace ns);
}
