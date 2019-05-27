package com.ms.silverking.cloud.toporing;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.ms.silverking.cloud.config.HostGroupTable;
import com.ms.silverking.cloud.dht.common.DHTUtil;
import com.ms.silverking.cloud.meta.ExclusionSet;
import com.ms.silverking.cloud.storagepolicy.StoragePolicy;
import com.ms.silverking.cloud.storagepolicy.StoragePolicyGroup;
import com.ms.silverking.cloud.topology.Node;
import com.ms.silverking.cloud.topology.NodeClass;
import com.ms.silverking.cloud.topology.Topology;
import com.ms.silverking.cloud.toporing.meta.WeightSpecifications;

/**
 * Immutable collection of all parameters necessary to create
 * a new RingTree.  
 */
public class RingTreeRecipe {
    public final Topology   topology;
    public final Node       ringParent;
    public final WeightSpecifications    weightSpecs;
    public final ExclusionSet   exclusionList;
    public final StoragePolicyGroup storagePolicyGroup;
    public final StoragePolicy  storagePolicy;
    public final HostGroupTable hostGroupTable;
    public final Set<String>    hostGroups;
    public final long			ringConfigVersion; 
    public final long			ringCreationTime;
    // Note that we don't have instance version information because that is a function of creation
    
    private static final boolean	debug = false;
    
    public RingTreeRecipe(Topology topology, Node ringParent, 
            WeightSpecifications weightSpecs, ExclusionSet exclusionList, 
            StoragePolicyGroup storagePolicyGroup, String storagePolicyName, 
            HostGroupTable hostGroupTable, Set<String> hostGroups,
            long ringConfigVersion, long ringCreationTime) {
    	Preconditions.checkNotNull(topology);
    	Preconditions.checkNotNull(ringParent);
        this.topology = topology;
        this.ringParent = ringParent;
        this.weightSpecs = evenlyDistributeWeights(topology, ringParent, weightSpecs, exclusionList, hostGroupTable, hostGroups);
        this.exclusionList = exclusionList;
        this.storagePolicyGroup = storagePolicyGroup;
        this.storagePolicy = storagePolicyGroup.getPolicy(storagePolicyName);
        if (storagePolicy == null) {
            throw new RuntimeException("Unable to find storage policy: "+ storagePolicyName);
        }
        this.hostGroupTable = hostGroupTable;
        this.hostGroups = hostGroups;
        this.ringConfigVersion = ringConfigVersion;
        this.ringCreationTime = ringCreationTime; 
    }
    
    public RingTreeRecipe(Topology topology, String ringParent, 
                        WeightSpecifications weightSpecs, ExclusionSet exclusionList, 
                        StoragePolicyGroup storagePolicyGroup, String storagePolicyName, 
                        HostGroupTable hostGroupTable, Set<String> hostGroups,
                        long ringConfigVersion, long ringCreationTime) {
        this(topology, topology.getNodeByID(ringParent), weightSpecs, exclusionList, 
                storagePolicyGroup, storagePolicyName, 
                hostGroupTable, hostGroups,
                ringConfigVersion, ringCreationTime);
        if (topology.getNodeByID(ringParent) == null) {
            throw new RuntimeException("Can't find parent with id: "+ ringParent);
        }
    }
    
    private static WeightSpecifications evenlyDistributeWeights(Topology topology, Node ringParent, WeightSpecifications weightSpecs, 
    															ExclusionSet exclusionList, HostGroupTable hostGroupTable, Set<String> hostGroups) {
    	Map<String,Double> nodeWeights;
    	
    	nodeWeights = new HashMap<>();
    	for (Map.Entry<String,Double> e : weightSpecs.getNodeWeights()) {
    		if (debug) {
    			System.out.printf("\t@@@@\t%s\t%f\n", e.getKey(), e.getValue());
    		}
    		nodeWeights.put(e.getKey(), e.getValue());
    	}
    	_evenlyDistributeWeights(topology, ringParent, nodeWeights, exclusionList, hostGroupTable, hostGroups);
    	return new WeightSpecifications(weightSpecs.getVersion(), nodeWeights);
    }
    
    public double getWeight(Node node) {
    	return weightSpecs.getWeight(node);
    }
    
    private static double _evenlyDistributeWeights(Topology topology, Node node, Map<String, Double> nodeWeights, 
    											ExclusionSet exclusionList, HostGroupTable hostGroupTable, Set<String> hostGroups) {
    	Double	weight;
    	
		weight = nodeWeights.get(node.getIDString());
		if (weight == null) {
	    	if (node.hasChildren()) {
	    		double	sum;
	    		
	    		sum = 0.0;
	    		for (Node child : node.getChildren()) {
	    			sum += _evenlyDistributeWeights(topology, child, nodeWeights, exclusionList, hostGroupTable, hostGroups);
	    		}
	    		weight = sum;
	    	} else {
	    		if (!Sets.intersection(hostGroupTable.getHostGroups(node.getIDString()), hostGroups).isEmpty() 
	    				&& (!exclusionList.contains(node.getIDString()))) {
	    			weight = WeightSpecifications.defaultWeight;
	    		} else {
	    			weight = 0.0;
	    		}
    		}
	    	nodeWeights.put(node.getIDString(), weight);
    	}
		if (debug) {
			System.out.printf("\t####\t%s\t%f\n", node.getIDString(), weight);
		}
		return weight;
	}

	public RingTreeRecipe newParentAndStoragePolicy(Node newParent, String storagePolicyName) {
        return new RingTreeRecipe(topology, newParent, weightSpecs, 
                                  exclusionList, storagePolicyGroup, storagePolicyName, hostGroupTable, hostGroups,
                                  ringConfigVersion, DHTUtil.currentTimeMillis());
    }
    
    public Set<Node> nonExcludedChildren(String nodeID) {
        ImmutableSet.Builder<Node>  builder;
        Node    node;
        
        node = topology.getNodeByID(nodeID);
        builder = ImmutableSet.builder();
        for (Node child : node.getChildren()) {
            if (!exclusionList.contains(child.getIDString())) {
                if (hasDescendantInHostGroups(child)) {
                    builder.add(child);
                }
            }
        }
        return builder.build();
    } 
    
    public boolean hasDescendantInHostGroups(Node node) {
        if (node.getNodeClass() == NodeClass.server) {
            return hostGroupTable.serverInHostGroupSet(node.getIDString(), hostGroups);
        } else {
            for (Node child : node.getChildren()) {
                if (hasDescendantInHostGroups(child)) {
                    return true;
                }
            }
            return false;
        }
    }

    @Override
    public String toString() {
        return topology +"\n"+ ringParent +"\n"+ weightSpecs 
               +"\n"+ exclusionList +"\n"+ storagePolicy;
    }
    
    /*
    public String toVersionString() {
        return topology.getVersion() 
                +":"+ weightSpecs.getVersion() 
                +":"+ exclusionList.getVersion() 
                +":"+ storagePolicyGroup.getVersion();
    }
    */
}
