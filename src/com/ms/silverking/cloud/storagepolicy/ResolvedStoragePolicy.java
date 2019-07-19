package com.ms.silverking.cloud.storagepolicy;

import java.util.List;

import com.ms.silverking.cloud.topology.Node;
import com.ms.silverking.cloud.toporing.RingTree;

/**
 * Consider deprecating
 * 
 * Associates a StoragePolicy definition with 
 * primary and secondary servers.
 */
public class ResolvedStoragePolicy {
    private final StoragePolicy storagePolicy;
    private final List<Node>    primaryServers;
    private final List<Node>    secondaryServers;
    
    public ResolvedStoragePolicy(StoragePolicy storagePolicy,
                                List<Node> primaryServers, 
                                List<Node> secondaryServers) {
        this.storagePolicy = storagePolicy;
        this.primaryServers = primaryServers;
        this.secondaryServers = secondaryServers;
    }
    
    public StoragePolicy getStoragePolicy() {
        return storagePolicy;
    }

    public List<Node> getPrimaryServers() {
        return primaryServers;
    }

    public List<Node> getSecondaryServers() {
        return secondaryServers;
    }
    
    // creation
    
    public static ResolvedStoragePolicy resolve(StoragePolicy storagePolicy, RingTree ringTree) {
        /*
        Node    root;
        
        root = topoRing.getTopology().getRoot();
        walk(topoRing, root);
        */
        return null;
    }
    
    private static void walk(RingTree ringTree, Node node) {
        System.out.println(node +"\t"+ ringTree.getMap(node.getIDString()));
        for (Node child : node.getChildren()) {
            walk(ringTree, child);
        }
    }
/*    
    public static ResolvedStoragePolicy resolve(StoragePolicy storagePolicy, Node node) {
        assert node != null;
        System.out.println(storagePolicy +" "+ node);
        if (!node.getNodeClass().equals(storagePolicy.getNodeClass())) {
            System.err.println(node.getNodeClass() +" != "+ storagePolicy.getNodeClass());
            throw new RuntimeException("Node class mismatch in policy resolution");
        } else {
            List<Node>  primaryServers;
            List<Node>  secondaryServers;
            
            primaryServers = new ArrayList<>();
            secondaryServers = new ArrayList<>();
            resolve(storagePolicy, node, primaryServers, secondaryServers);
            return new ResolvedStoragePolicy(storagePolicy, primaryServers, secondaryServers);
        }
    }
    
    private static void resolve(StoragePolicy storagePolicy, Node node,
                            List<Node> primaryServers, List<Node> secondaryServers) {
        if (!node.getNodeClass().equals(storagePolicy.getNodeClass())) {
            throw new RuntimeException("Node class mismatch in policy resolution");
        } else {
            List<Node>  children;
            Policy      primaryPolicy;
            Policy      secondaryPolicy;
            
            // what does it mean to be secondarily stored in the hierarchy

            primaryPolicy = storagePolicy.getPrimaryPolicy();
            secondaryPolicy = storagePolicy.getSecondaryPolicy();
            children = node.getChildren();
            if (children.size() < primaryPolicy.getNumber()) {
                // FUTURE - possibly throw checked exception to allow
                // higher-level code to wait when this happens
                throw new RuntimeException("children.size() < primaryPolicy.getNumber()");
            }
            if (children.size() > 0) {
                NodeClass   childrenClass;
                
                childrenClass = children.get(0).getNodeClass();
                if (!childrenClass.equals(NodeClass.server)) {
                    for (int i = 0; i < primaryPolicy.getNumber(); i++) {
                        resolve(primaryPolicy.getStoragePolicy(), children.get(i), primaryServers, secondaryServers);
                        // FUTURE - how about secondary?
                    }
                } else {
                    int i;
                    
                    i = 0;
                    while (i < primaryPolicy.getNumber()) {
                        primaryServers.add(children.get(i));
                        i++;
                    }
                    while (i < secondaryPolicy.getNumber()) {
                        secondaryServers.add(children.get(i));
                        i++;
                    }
                }
            } 
        }
    }
    */
}
