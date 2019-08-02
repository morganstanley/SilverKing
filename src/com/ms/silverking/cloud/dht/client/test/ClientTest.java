package com.ms.silverking.cloud.dht.client.test;

import com.ms.silverking.cloud.dht.ConsistencyProtocol;
import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.RevisionMode;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.Namespace;
import com.ms.silverking.collection.Pair;

public interface ClientTest {
    public String getTestName();
    public ConsistencyProtocol getConsistencyProtocol();
    public NamespaceVersionMode getNamespaceVersionMode();
    public RevisionMode getRevisionMode();
    public Pair<Integer,Integer> runTest(DHTSession session, Namespace ns);
}
