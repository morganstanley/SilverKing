package com.ms.silverking.cloud.toporing;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
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
    
    public RingTreeRecipe(Topology topology, Node ringParent, 
            WeightSpecifications weightSpecs, ExclusionSet exclusionList, 
            StoragePolicyGroup storagePolicyGroup, String storagePolicyName, 
            HostGroupTable hostGroupTable, Set<String> hostGroups,
            long ringConfigVersion, long ringCreationTime) {
        this.topology = topology;
        this.ringParent = ringParent;
        this.weightSpecs = weightSpecs;
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
