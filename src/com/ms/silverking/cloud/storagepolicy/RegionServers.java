package com.ms.silverking.cloud.storagepolicy;

import java.util.List;

import com.ms.silverking.cloud.topology.Node;

public class RegionServers {
    private final List<Node>    primary;
    private final List<Node>    secondary;

    public RegionServers(List<Node> primary, List<Node> secondary) {
        this.primary = primary;
        this.secondary = secondary;
    }
    
    private void addList(StringBuilder sb, String name, List<Node> nodes) {
        sb.append(name);
        sb.append('\n');
        for (Node node : nodes) {
            sb.append(node);
            sb.append(' ');
        }
        sb.append('\n');
    }
        
    @Override
    public String toString() {
        StringBuilder   sb;
        
        sb = new StringBuilder();
        addList(sb, "Primary", primary);
        addList(sb, "Secondary", secondary);
        return sb.toString();
    }
}
