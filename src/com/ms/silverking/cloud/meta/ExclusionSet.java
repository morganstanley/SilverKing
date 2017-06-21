package com.ms.silverking.cloud.meta;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.topology.Node;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.net.IPAndPort;

public class ExclusionSet extends ServerSetExtension implements ZKVersionedDefinition {
	private final long	mzxid;
	
	public static final String	singleLineDelimiter = ",";
	
	private static final long	INVALID_ZXID = -1;
	
    public ExclusionSet(ServerSet serverSet, long mzxid) {
        super(serverSet);
        this.mzxid = mzxid;
    }
    
    public ExclusionSet(ServerSet serverSet) {
    	this(serverSet, INVALID_ZXID);
    }
    
    private ExclusionSet(long version) {
    	this (new ServerSet(new HashSet<String>(), version));
    }
    
    public ExclusionSet(Set<String> excludedEntities, long version, long mzxid) {
        this(new ServerSet(excludedEntities, version), mzxid);
    }
    
	@Override
	public long getMzxid() {
		return mzxid;
	}	
    
    public static ExclusionSet emptyExclusionSet(long version) {
    	return new ExclusionSet(version);
    }
    
    public ExclusionSet add(Set<String> newExcludedEntities) {
        return new ExclusionSet(serverSet.add(newExcludedEntities));
    }
    
    public ExclusionSet addByIPAndPort(Set<IPAndPort> newExcludedEntities) {
    	Set<String>	s;
    	
    	s = new HashSet<>();
    	for (IPAndPort e : newExcludedEntities) {
    		s.add(e.getIPAsString());
    	}
    	return add(s);
    }
    
    public Set<IPAndPort> asIPAndPortSet(int port) {
    	Set<IPAndPort>	s;
    	
    	s = new HashSet<>();
    	for (String server : serverSet.getServers()) {
    		s.add(new IPAndPort(server, port));
    	}
    	return ImmutableSet.copyOf(s);
    }
    
    public ExclusionSet remove(Set<String> newIncludedEntities) {
        return new ExclusionSet(serverSet.remove(newIncludedEntities));
    }
    
    public List<Node> filter(List<Node> raw) {
        List<Node>    filtered;
        
        filtered = new ArrayList<>(raw.size());
        for (Node node : raw) {
            if (!getServers().contains(node.getIDString())) {
                filtered.add(node);
            }
        }
        return filtered;
    }

    public List<IPAndPort> filterByIP(Collection<IPAndPort> raw) {
        List<IPAndPort>    filtered;
        
        filtered = new ArrayList<>(raw.size());
        for (IPAndPort node : raw) {
            boolean excluded;
            
            excluded = false;
            for (String server : getServers()) {
                if (node.getIPAsString().equals(server)) {
                    excluded = true;
                    break;
                }
            }
            if (!excluded) {
                filtered.add(node);
            }
        }
        return filtered;
    }
    
    public static ExclusionSet parse(String def) throws IOException {
        return new ExclusionSet(new ServerSet(CollectionUtil.parseSet(def, singleLineDelimiter), VersionedDefinition.NO_VERSION));
    }
    
    public static ExclusionSet parse(File file) throws IOException {
        return new ExclusionSet(ServerSet.parse(new FileInputStream(file), VersionedDefinition.NO_VERSION));
    }

	public static ExclusionSet union(ExclusionSet s1, ExclusionSet s2) {
		ExclusionSet	u;
		
		u = emptyExclusionSet(0);
		u = u.add(s1.getServers());
		u = u.add(s2.getServers());
		return u;
	}
}
