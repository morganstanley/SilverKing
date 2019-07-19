package com.ms.silverking.cloud.storagepolicy;

import com.ms.silverking.cloud.topology.NodeClass;

public class NodeClassAndStoragePolicyName extends NodeClassAndName {
    public NodeClassAndStoragePolicyName(NodeClass nodeClass, String storagePolicyName) {
        super(nodeClass, storagePolicyName);
    }
    
    public static NodeClassAndStoragePolicyName parse(String s) throws PolicyParseException {
        NodeClassAndName    n;
        
        n = NodeClassAndName.parse(s);
        if (n.getName() != null) {
            if (n.getNodeClass() == NodeClass.server) {
                System.err.println("parsing: "+ s);
                throw new PolicyParseException("name not allowed for class "+ n.getNodeClass());
            }
        } else {
            if (n.getNodeClass() != NodeClass.server) {
                System.err.println("parsing: "+ s);
                throw new PolicyParseException("name required for class "+ n.getNodeClass());
            }
        }
        return new NodeClassAndStoragePolicyName(n.getNodeClass(), n.getName());
    }
    
    public NodeClass getNodeClass() {
        return super.getNodeClass();
    }

    public String getStoragePolicyName() {
        return super.getName();
    }
    
    @Override
    public String toString() {
        if (getStoragePolicyName() != null) {
            return getNodeClass() +":"+ getStoragePolicyName();
        } else {
            return getNodeClass().toString();
        }
    }
}
