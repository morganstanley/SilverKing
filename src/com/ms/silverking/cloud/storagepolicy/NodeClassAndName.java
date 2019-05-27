package com.ms.silverking.cloud.storagepolicy;

import com.ms.silverking.cloud.topology.NodeClass;

public class NodeClassAndName {
    private final NodeClass nodeClass;
    private final String    name;
    
    public NodeClassAndName(NodeClass nodeClass, String name) {
        this.nodeClass = nodeClass;
        this.name = name;
    }
    
    public static NodeClassAndName parse(String s) throws PolicyParseException {
        String[]    tokens;
        
        tokens = s.split(":");
        if (tokens.length > 2) {
            throw new PolicyParseException("Bad NodeClassAndName: "+ s);
        } else {
            NodeClass   nodeClass;
            String      name;
            
            nodeClass = NodeClass.forName(tokens[0]);
            if (nodeClass == null) {
                throw new PolicyParseException("Bad NodeClass: "+ tokens[0]);
            }
            if (tokens.length > 1) {
                name = tokens[1];
            } else {
                name = null;
            }
            return new NodeClassAndName(nodeClass, name);
        }
    }
    
    public NodeClass getNodeClass() {
        return nodeClass;
    }

    public String getName() {
        return name;
    }
    
    @Override
    public String toString() {
        return nodeClass +":"+ name;
    }
}
