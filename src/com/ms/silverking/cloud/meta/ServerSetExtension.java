package com.ms.silverking.cloud.meta;

import java.util.Set;

/**
 * Common functionality used by ExclusionSet and PassiveNodeSet to wrap ServerSet.
 */
abstract class ServerSetExtension implements VersionedDefinition {
    protected final ServerSet   serverSet;
    
    ServerSetExtension(ServerSet serverSet) {
        this.serverSet = serverSet;
    }
    
    public int size() {
    	return serverSet.size();
    }
    
    public Set<String> getServers() {
        return serverSet.getServers();
    }
    
    @Override
    public long getVersion() {
        return serverSet.getVersion();
    }
    
    public boolean contains(String serverID) {
        return serverSet.contains(serverID);
    }
    
    @Override
    public boolean equals(Object o) {
        return serverSet.equals(((ServerSetExtension)o).serverSet);
    }
    
    @Override
    public String toString() {
        return serverSet.toString();
    }
}
