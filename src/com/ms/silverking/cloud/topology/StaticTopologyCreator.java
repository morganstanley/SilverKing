package com.ms.silverking.cloud.topology;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.ImmutableList;

public class StaticTopologyCreator {
	public static final String	parentID = "static_rack";
	
	public static Topology createTopology(String id, Collection<String> servers) {
		GenericNode	root;
		List<Node>	children;
		
		children = new ArrayList<>(servers.size());
		for (String serverID : servers) {
			children.add(new GenericNode(NodeClass.server, serverID));
		}
		root = new GenericNode(NodeClass.rack, parentID, children);
		return Topology.fromRoot(id, root);
	}

	public static void main(String[] args) {
		System.out.println(createTopology(null, ImmutableList.copyOf(args)));
	}
}
