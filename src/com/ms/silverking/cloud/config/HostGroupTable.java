package com.ms.silverking.cloud.config;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.net.InetAddresses;
import com.ms.silverking.cloud.meta.VersionedDefinition;
import com.ms.silverking.collection.HashedSetMap;
import com.ms.silverking.net.InetAddressComparator;

public class HostGroupTable implements VersionedDefinition, Serializable {
	private final HashedSetMap<String, InetAddress> groupToServerMap;
	private final HashedSetMap<String, String>	    groupToServerAddressMap;
    private final HashedSetMap<InetAddress, String> serverToGroupMap;
	private final long                              version;
	
	private static final long serialVersionUID = 7151811840390960914L;
	
	public HostGroupTable(HashedSetMap<String, InetAddress> groupToServerMap, long version) {
		this.groupToServerMap = groupToServerMap;
		this.version = version;
        this.groupToServerAddressMap = createGroupToServerAddressMap(groupToServerMap);
		this.serverToGroupMap = createServerToGroupMap(groupToServerMap);
	}
	
	public static HostGroupTable of(String group, InetAddress server, long version) {
		HashedSetMap<String,InetAddress>	m;
		
		m = new HashedSetMap<>();
		m.addValue(group, server);
		return new HostGroupTable(m, version);
	}
	
	private static HashedSetMap<InetAddress, String> createServerToGroupMap(HashedSetMap<String, InetAddress> groupToServerMap) {
	    HashedSetMap<InetAddress, String>  serverToGroupMap;
	    
	    serverToGroupMap = new HashedSetMap<>();
        for (String group : groupToServerMap.getKeys()) {
            for (InetAddress server : groupToServerMap.getSet(group)) {
                serverToGroupMap.addValue(server, group);
            }
        }
	    return serverToGroupMap;
	}
	
    private static HashedSetMap<String, String> createGroupToServerAddressMap(HashedSetMap<String, InetAddress> groupToServerMap) {
        HashedSetMap<String, String>    groupToServerAddressMap;
        
        groupToServerAddressMap = new HashedSetMap<>();
        for (String group : groupToServerMap.getKeys()) {
            for (InetAddress server : groupToServerMap.getSet(group)) {
                groupToServerAddressMap.addValue(group, server.getHostAddress());
            }
        }
        return groupToServerAddressMap;
    }
    
	public Set<InetAddress> getHosts(Iterable<String> groupNames) {
	    ImmutableSet.Builder<InetAddress>  hosts;
	    
	    hosts = ImmutableSet.builder();
	    for (String groupName : groupNames) {
	        hosts.addAll(getHosts(groupName));
	    }
	    return hosts.build();
	}
	
    public Set<String> getHostAddresses(Iterable<String> groupNames) {
        ImmutableSet.Builder<String>  hosts;
        
        hosts = ImmutableSet.builder();
        for (String groupName : groupNames) {
            hosts.addAll(getHostAddresses(groupName));
        }
        return hosts.build();
    }
    
    public Set<InetAddress> getHosts(String groupName) {
        Set<InetAddress>    s;
        
        s = groupToServerMap.getSet(groupName);
        if (s == null) {
            s = ImmutableSet.of();
        }
        return s;
    }
    
    public Set<String> getHostAddresses(String groupName) {
        Set<String> s;
        
        s = groupToServerAddressMap.getSet(groupName);
        if (s == null) {
            s = ImmutableSet.of();
        }
        return s;
    }
    
    public boolean serverInHostGroupSet(String server, Set<String> hostGroups) {
        Set<String> serverHostGroups;
        
        serverHostGroups = getHostGroups(server);
        for (String serverHostGroup : serverHostGroups) {
            if (hostGroups.contains(serverHostGroup)) {
                return true;
            }
        }
        return false;
    }
    
    public Set<String> getHostGroups(String server) {
        try {
            Set<String> hostGroups;
            
            hostGroups = getHostGroups(InetAddress.getByName(server));
            if (hostGroups == null) {
                hostGroups = ImmutableSet.of();
            }
            return hostGroups;
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }
    
    public Set<String> getHostGroups(InetAddress server) {
        return serverToGroupMap.getSet(server);
    }
    
    @Override
    public long getVersion() {
        return version;
    }
	
    public static HostGroupTable parse(String def, long version) {
        try {
            return parse(new ByteArrayInputStream(def.getBytes()), version);
        } catch (IOException ioe) {
            throw new RuntimeException("Unexpected exception", ioe);
        }
    }
    
	public static HostGroupTable parse(File fileName, long version) throws IOException {
		return parse(new FileInputStream(fileName), version);
	}
	
	public static HostGroupTable parse(InputStream inStream, long version) throws IOException {
		try {
			BufferedReader	reader;
			String			line;
			HashedSetMap<String, InetAddress>	hostGroupMap;
			
			hostGroupMap = new HashedSetMap<String, InetAddress>();
			reader = new BufferedReader(new InputStreamReader(inStream));
			do {
				line = reader.readLine();
				readLine(line, hostGroupMap);
			} while (line != null);
			return new HostGroupTable(hostGroupMap, version);
		} finally {
			inStream.close();
		}
	}
	
	private static void readLine(String line, HashedSetMap<String, InetAddress> hostGroupMap) {
		if (line != null) {
			line = line.trim();
			if (line.length() > 0) {
				String		ipToken;
				byte[]		ip;
				Set<String>	hostGroups;
				String[]	tokens;
			
				tokens = line.split("\\s+");
				ipToken = tokens[0];
				hostGroups = new HashSet<String>();
				for (int i = 1; i < tokens.length; i++) {
					hostGroups.add(tokens[i]);
				}
				for (String hostGroup : hostGroups) {
					hostGroupMap.addValue(hostGroup, InetAddresses.forString(ipToken));
				}
			}
		}
	}
	
	public static HostGroupTable createHostGroupTable(Collection<String> hostIPs, String groupName) throws UnknownHostException {
		HashedSetMap<String, InetAddress>	hostGroupMap;
		
		hostGroupMap = new HashedSetMap<>();
		for (String hostIP : hostIPs) {
			hostGroupMap.addValue(groupName, InetAddress.getByName(hostIP));
		}
		return new HostGroupTable(hostGroupMap, 0);
	}
	
	@Override
	public String toString() {
	    StringBuilder  sb;
	    List<InetAddress>  servers;
	    
	    servers = serverToGroupMap.getKeys();
	    Collections.sort(servers, new InetAddressComparator());
	    
	    sb = new StringBuilder();
        for (InetAddress server : servers) {
            sb.append(server.toString().substring(1));
            for (String group : serverToGroupMap.getSet(server)) {
                sb.append("\t"+ group);
            }
            sb.append('\n');
        }
        return sb.toString();
	}
	
    public String toGroupString() {
        StringBuilder  sb;
        
        sb = new StringBuilder();
        for (String group : groupToServerMap.getKeys()) {
            for (InetAddress host : groupToServerMap.getSet(group)) {
                sb.append("\t"+ host);
            }
            sb.append('\n');
        }
        return sb.toString();
    }
    
	// for unit testing
	public static void main(String[] args) {
		try {
			if (args.length != 1) {
				System.err.println("<file>");
			} else {
				HostGroupTable	table;
				
				table = HostGroupTable.parse(new File(args[0]), 0);
				System.out.println(table.toString());
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
}
