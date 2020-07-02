package com.ms.silverking.cloud.dht.util;

import com.ms.silverking.cloud.dht.client.ClientDHTConfiguration;
import com.ms.silverking.cloud.dht.daemon.DHTNode;
import com.ms.silverking.cloud.dht.daemon.DHTNodeConfiguration;
import com.ms.silverking.cloud.dht.daemon.storage.ReapPolicy;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;

public class TestDHTNode extends DHTNode {
    private ClientDHTConfiguration clientDHTConfiguration;
    private DHTNodeConfiguration nodeConfig;

    public TestDHTNode(String dhtName, ZooKeeperConfig zkConfig, DHTNodeConfiguration nodeConfig, ClientDHTConfiguration conf, int inactiveNodeTimeoutSeconds, ReapPolicy reapPolicy) {
        super(dhtName, zkConfig, nodeConfig, inactiveNodeTimeoutSeconds, reapPolicy, 0, null);
        this.nodeConfig = nodeConfig;
        this.clientDHTConfiguration = conf;
    }

    public ClientDHTConfiguration getClientDHTConfiguration() {
        return clientDHTConfiguration;
    }

    public void setClientDHTConfiguration(ClientDHTConfiguration conf) {
        clientDHTConfiguration = conf;
    }

    public DHTNodeConfiguration getDHTNodeConfiguration() {
        return nodeConfig;
    }

    public void setDHTNodeConfiguration(DHTNodeConfiguration nodeConfig) {
        this.nodeConfig = nodeConfig;
    }


}
