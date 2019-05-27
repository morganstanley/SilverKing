package com.ms.silverking.cloud.topology;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class NodeClass {
	private final String	name;
	
	private static final ConcurrentMap<String, NodeClass>	nameToClassMap;
	
	static {
		nameToClassMap = new ConcurrentHashMap<String, NodeClass>();
	}
	
    public static final NodeClass  server = forName("Server");
    public static final NodeClass  rack = forName("Rack");
    public static final NodeClass  domain = forName("Domain");
    public static final NodeClass  pod = forName("Pod");
    public static final NodeClass  datacenter = forName("Datacenter");
    public static final NodeClass  region = forName("Region");
    public static final NodeClass  continent = forName("Continent");
    public static final NodeClass  global = forName("Global");
    
	public static NodeClass forName(String name) {
		NodeClass	nodeClass;
		
		verifyNotPresent(name, ' ');
        verifyNotPresent(name, '\n');
        verifyNotPresent(name, '\t');
		nodeClass = nameToClassMap.get(name);
		if (nodeClass == null) {
		    NodeClass existing;
		    
		    nodeClass = new NodeClass(name);
			existing = nameToClassMap.putIfAbsent(name, nodeClass);
			if (existing != null) {
			    nodeClass = existing;
			}
		}
		return nodeClass;
	}
	
	private static void verifyNotPresent(String name, char ch) {
        if (name.indexOf(ch) >= 0) {
            throw new RuntimeException("Illegal NodeClass: "+ name);
        }
	}
	
	private NodeClass(String name) {
		this.name = name;
	}
	
	public String getName() {
		return name;
	}
	
	@Override 
	public boolean equals(Object obj) {
		NodeClass	oNodeClass;
		
		oNodeClass = (NodeClass)obj;
		return name.equals(oNodeClass.name);
	}
	
	@Override
	public int hashCode() {
		return name.hashCode();
	}
	
	@Override
	public String toString() {
		return name;
	}
}
