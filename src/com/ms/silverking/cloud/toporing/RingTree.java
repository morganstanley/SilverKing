package com.ms.silverking.cloud.toporing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.common.OwnerQueryMode;
import com.ms.silverking.cloud.dht.daemon.DHTNode;
import com.ms.silverking.cloud.dht.daemon.ReplicaPrioritizer;
import com.ms.silverking.cloud.ring.IntersectionResult;
import com.ms.silverking.cloud.ring.LongRingspace;
import com.ms.silverking.cloud.ring.RingRegion;
import com.ms.silverking.cloud.storagepolicy.StoragePolicy;
import com.ms.silverking.cloud.topology.Node;
import com.ms.silverking.cloud.topology.NodeClass;
import com.ms.silverking.cloud.topology.Topology;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;

/**
 * Tree of TopologyRings for a topology. Every parent in the topology
 * has an associated TopologyRing for its children.
 * 
 * Each of these TopologyRings is retrieved using the id of the
 * ring which is the path to the ring.
 * 
 * StorageNodes may be retrieved for any ring. Rings that
 * are at higher levels are composed with the lower-level
 * rings to compute the storage nodes.
 */
public class RingTree {
    private final Topology  topology;
    //private final Map<Pair<String,String>,TopologyRing>    maps;
    private final Map<String,TopologyRing>    maps;
    private final long	ringConfigVersion;
    // Note that we don't store instance version information as that isn't known for trees that haven't been written to zookeeper
    // InstantiatedRingTree contains this information for trees that have been written to zookeeper.
    private final long  ringCreationTime;
    
    private static final int    allowedError = 10000;
    private static final int    allowedContiquityError = 10;
        
    private boolean debug = false;
    
    public RingTree(Topology topology, Map<String,TopologyRing> maps, long ringConfigVersion, long ringCreationTime) {
        this.topology = topology;
        this.maps = maps;
        this.ringConfigVersion = ringConfigVersion;
        this.ringCreationTime = ringCreationTime;
    }
    
    public Topology getTopology() {
        return topology;
    }
    
    public Map<String,TopologyRing> getMaps() {
        return maps;
    }
    
    public long getRingConfigVersion() {
        return ringConfigVersion;
    }
    
    public long getRingCreationTime() {
        return ringCreationTime;
    }
    
    //public TopologyRing getMap(String parentID, String storagePolicyName) {
    //    System.out.println("getMap: "+ parentID +" "+ storagePolicyName);
    //    return maps.get(new Pair<>(parentID, storagePolicyName));
    //}
    public TopologyRing getMap(String parentID) {
        if (debug) {
            System.out.println("getMap: "+ parentID);
        }
        return maps.get(parentID);
    }
    
    public Set<IPAndPort> getStorageReplicas(long coordinate) {
        Set<Node>   nodes;
        Set<IPAndPort>    replicas;
        
        if (debug) {
            System.out.println("getStorageReplicas");
        }
        nodes = getStorageNodes(coordinate);
        if (debug) {
            System.out.println("nodes.size()"+ nodes.size());
        }
        replicas = new HashSet<>();
        for (Node node : nodes) {
            if (debug) {
                System.out.println(node);
            }
            replicas.add(new IPAndPort(node.getIDString() +":"+ DHTNode.getServerPort()));
        }
        return ImmutableSet.copyOf(replicas);
    }
    
    public Set<Node> getStorageNodes(long coordinate) {
        return getStorageNodes(topology.getRoot(), coordinate);
    }
    
    public List<Node> getStorageNodesOrdered(long coordinate, String pointOfView) {
        return getStorageNodesOrdered(topology.getRoot(), coordinate, pointOfView);
    }
    
    private List<Node> getStorageNodesOrdered(Node parent, long coordinate, String pointOfView) {
        List<Node>  storageNodes;
        
        storageNodes = new ArrayList<>(getStorageNodes(parent, coordinate));
        Collections.sort(storageNodes, new NodeDistanceComparator(pointOfView));
        return storageNodes;
    }
    
    // Slow/proof-of-concept implementation
    // replace, possibly pre-compute when concept is 
    // decided on
    // (Precomputation is now in place)
    private Set<Node> getStorageNodes(Node parent, long coordinate) {
        TopologyRing    ring;
        RingEntry       ringEntry;
        Set<Node>       children;
        Set<Node>       storageNodes;
        long            normalizedCoordinate;
        
        ring = getNodeRing(parent);
        ringEntry = ring.getOwner(coordinate);
        normalizedCoordinate = LongRingspace.mapRegionPointToRingspace(ringEntry.getRegion(), coordinate);
        children = ringEntry.getPrimaryOwnersSet();
        storageNodes = new HashSet<>();
        for (Node child : children) {
            if (child.hasChildren()) {
                storageNodes.addAll(getStorageNodes(child, normalizedCoordinate));
            } else {
                storageNodes.add(child);
            }
        }
        return storageNodes;
    }
    
    private TopologyRing getNodeRing(Node node) {
        return maps.get(node.getIDString());
    }
    
    /////////////////////////////////
    
    public ResolvedReplicaMap getResolvedMap(String ringParentName, ReplicaPrioritizer replicaPrioritizer) {
    	try {
	        ResolvedReplicaMap  resolvedMap;
	        List<RingEntry>     entryList;
	        Node				node;
	        Stopwatch			sw;
	        
	        Log.warningf("getResolvedMap: %s", ringParentName);
	        sw = new SimpleStopwatch();
	        resolvedMap = new ResolvedReplicaMap(replicaPrioritizer);
	        if (debug) {
	            System.out.println("getResolvedMap: "+ topology.getRoot());
	        }
	        node = topology.getNodeByID(ringParentName);
	        if (node == null) {
	        	throw new RuntimeException("Unable to getNodeByID "+ ringParentName);
	        }
	        entryList = project(node, LongRingspace.globalRegion);
	        for (RingEntry entry : entryList) {
	            resolvedMap.addEntry(entry);
	        }
	        resolvedMap.computeReplicaSet();
	        sw.stop();
	        Log.warningf("getResolvedMap: %s complete %f", ringParentName, sw.getElapsedSeconds());
	        return resolvedMap;
    	} catch (RuntimeException re) {
    		re.printStackTrace();
    		throw re;
    	}
    }

    private List<RingEntry> project(Node node, RingRegion parentRegion) {
        List<RingEntry> entryList;
        List<RingEntry> projectedEntryList;
        List<RingEntry> cleanedEntryList;
        
        if (debug) {
            System.out.println("project "+ node +" "+ parentRegion);
        }
        if (node.childNodeClassMatches(NodeClass.server)) {
            entryList = getRawEntryList(node);
        } else {
            if (!node.hasChildren()) {
                return new ArrayList<>();
            } else {
                TopologyRing    ring;
                StoragePolicy   storagePolicy;
                
                entryList = new ArrayList<>();
                ring = getNodeRing(node);
                if (ring == null) {
                    throw new RuntimeException("Can't find ring for node: "+ node);
                } else {
                    List<RingEntry> allChildList;
                    
                    allChildList = new ArrayList<>();
                    for (RingEntry entry : ring.getMembers()) {
                        // For all entries, go through all nodes and project
                        // all subentries to this entry
                        if (debug) {
                            System.out.println("primary");
                        }
                        for (Node childNode : entry.getPrimaryOwnersList()) {
                            List<RingEntry> childList;
                            
                            childList = project(childNode, entry.getRegion());
                            if (debug) {
                                System.out.println("back to "+ node +" from "+ childNode);
                            }
                            allChildList.addAll(childList);
                            //merge(entryList, childList);
                        }
                        
                        
                        if (debug) {
                            System.out.println("secondary");
                        }
                        for (Node childNode : entry.getSecondaryOwnersList()) {
                            List<RingEntry> childList;
                            
                            childList = convertPrimaryToSecondary(project(childNode, entry.getRegion()));
                            if (debug) {
                                System.out.println("back to "+ node +" from "+ childNode);
                            }
                            allChildList.addAll(childList);
                            //merge(entryList, childList);
                        }
                    }
                    merge(entryList, allChildList);
                }
            }
        }
        displayForDebug(entryList, "entryList");
        RingEntry.ensureEntryRegionsDisjoint(entryList);
        projectedEntryList = projectEntryList(entryList, parentRegion);
        displayForDebug(projectedEntryList, "projectedEntryList");
        cleanedEntryList = cleanupList(parentRegion, projectedEntryList);
        displayForDebug(cleanedEntryList, "cleanedEntryList");
        return cleanedEntryList;
    }
    
    /*
    private List<RingEntry> project(Node node, RingRegion parentRegion) {
        List<RingEntry> entryList;
        List<RingEntry> projectedEntryList;
        List<RingEntry> cleanedEntryList;
        
        if (debug) {
            System.out.println("project "+ node +" "+ parentRegion);
        }
        if (node.childNodeClassMatches(NodeClass.server)) {
            entryList = getRawEntryList(node);
        } else {
            if (!node.hasChildren()) {
                return new ArrayList<>();
            } else {
                TopologyRing    ring;
                StoragePolicy   storagePolicy;
                
                entryList = new ArrayList<>();
                ring = getNodeRing(node);
                if (ring == null) {
                    throw new RuntimeException("Can't find ring for node: "+ node);
                } else {
                    for (RingEntry entry : ring.getMembers()) {
                        // For all entries, go through all nodes and project
                        // all subentries to this entry
                        if (debug) {
                            System.out.println("primary");
                        }
                        for (Node childNode : entry.getPrimaryOwnersList()) {
                            List<RingEntry> childList;
                            
                            childList = project(childNode, entry.getRegion());
                            if (debug) {
                                System.out.println("back to "+ node +" from "+ childNode);
                            }
                            merge(entryList, childList);
                        }
                        
                        
                        if (debug) {
                            System.out.println("secondary");
                        }
                        for (Node childNode : entry.getSecondaryOwnersList()) {
                            List<RingEntry> childList;
                            
                            childList = convertPrimaryToSecondary(project(childNode, entry.getRegion()));
                            if (debug) {
                                System.out.println("back to "+ node +" from "+ childNode);
                            }
                            merge(entryList, childList);
                        }
                    }
                }
            }
        }
        displayForDebug(entryList, "entryList");
        RingEntry.ensureEntryRegionsDisjoint(entryList);
        projectedEntryList = projectEntryList(entryList, parentRegion);
        displayForDebug(projectedEntryList, "projectedEntryList");
        cleanedEntryList = cleanupList(parentRegion, projectedEntryList);
        displayForDebug(cleanedEntryList, "cleanedEntryList");
        return cleanedEntryList;
    }
    */
        
    private void displayForDebug(List<RingEntry> list, String name) {
        if (debug) {
            System.out.printf("start %s\n", name);
            System.out.printf("%s\n", RingEntry.toString(list, "\n"));
            System.out.printf("end %s\n", name);
        }
    }
    
    private List<RingEntry> convertPrimaryToSecondary(List<RingEntry> list) {
        List<RingEntry> cList;
        
        cList = new ArrayList<>(list.size());
        for (RingEntry entry : list) {
            cList.add(entry.convertPrimaryToSecondary());
        }
        return cList;
    }
    
    
    private void merge(List<RingEntry> destList, List<RingEntry> _sourceList) {
        List<RingEntry> sourceList;
        
        Collections.sort(destList, RingEntryPositionComparator.instance);
        
        if (_sourceList.size() == 0) {
            throw new RuntimeException("Unexpected empty source list");
        }
        if (debug) {
            System.out.println("merge: ************************");
            System.out.println("\t\t"+ RingEntry.toString(destList, "\n"));
            System.out.println("...................................");
            System.out.println("\t\t"+ RingEntry.toString(_sourceList, "\n"));
            System.out.println("===================================");
        }
        RingEntry.ensureEntryRegionsDisjoint(destList);
        //RingEntry.ensureEntryRegionsDisjoint(_sourceList); // c/o since we support non-disjoint now
        sourceList = new ArrayList<>(_sourceList);
        while (sourceList.size() > 0) {
            RingEntry   oldSourceEntry;
            RingRegion  oldSourceRegion;
            boolean     destScanActive;
            int			searchResult;
            int			startIndex;
            int			endIndex;
            int			insertionIndex;
            
            oldSourceEntry = sourceList.remove(0);
            oldSourceRegion = oldSourceEntry.getRegion();

            searchResult = Collections.binarySearch(destList, oldSourceEntry, RingEntryPositionComparator.instance);
            if (searchResult < 0) {
            	// no exact match for this position was found
            	// we can look at the two entries next to us to figure out what's up
            	insertionIndex = -(searchResult + 1);
            	startIndex = insertionIndex - 1;
            	if (startIndex < 0) {
            		startIndex = 0;
            		endIndex = destList.size();
            	} else {
                	endIndex = startIndex + 1;
            	}
            } else {
            	// we found an exact match for this position, there will be some sort of match below
        		insertionIndex = Integer.MIN_VALUE; // should have perfect match, no insertion
            	startIndex = searchResult;
            	endIndex = searchResult;
            }
            
            // For simplicity, we perform a naive loop through all dests even though
            // it would be possible to avoid this loop.
            destScanActive = true;
            for (int destIndex = startIndex; destScanActive && destIndex <= endIndex && destIndex < destList.size();) {
                RingEntry           oldDestEntry;
                RingRegion          oldDestRegion;
                IntersectionResult  iResult;
                
                oldDestEntry = destList.get(destIndex);
                oldDestRegion = oldDestEntry.getRegion();
                
                iResult = RingRegion.intersect(oldDestRegion, oldSourceRegion);
                // Note below when modifying destList, we add in the reverse order of where 
                // we want the new entries to end up so that we can use destIndex for all 
                // and avoid computing how many are added by each step
                switch (iResult.getIntersectionType()) {
                case disjoint:
                    destIndex++;
                    break;
                case isomorphic:
                    destList.remove(destIndex);                    
                    destList.add(destIndex, oldDestEntry.addOwners(oldSourceEntry));
                    destScanActive = false; // go on to next source entry 
                    break;
                case abPartial: 
                    destList.remove(destIndex);
                    destList.add(destIndex, oldDestEntry.replaceRegion(iResult.getOverlapping()).addOwners(oldSourceEntry));
                    destList.add(destIndex, oldDestEntry.replaceRegion(iResult.getANonOverlapping()));
                    sourceList.add(0, oldSourceEntry.replaceRegion(iResult.getBNonOverlapping()));
                    destScanActive = false; // go on to next source entry 
                    break;
                case baPartial:
                    destList.remove(destIndex);
                    destList.add(destIndex, oldDestEntry.replaceRegion(iResult.getANonOverlapping()));
                    destList.add(destIndex, oldDestEntry.replaceRegion(iResult.getOverlapping()).addOwners(oldSourceEntry));
                    sourceList.add(0, oldSourceEntry.replaceRegion(iResult.getBNonOverlapping()));
                    destScanActive = false; // go on to next source entry 
                    break;
                case aSubsumesB: // fall through
                    destList.remove(destIndex);
                    if (iResult.getANonOverlapping().size() == 1) {
                        if (oldDestEntry.getRegion().getStart() == oldSourceEntry.getRegion().getStart()) {
                            destList.add(destIndex, oldDestEntry.replaceRegion(iResult.getANonOverlapping()));
                            destList.add(destIndex, oldDestEntry.replaceRegion(iResult.getOverlapping()).addOwners(oldSourceEntry));
                        } else if (oldDestEntry.getRegion().getEnd() == oldSourceEntry.getRegion().getEnd()) {
                            destList.add(destIndex, oldDestEntry.replaceRegion(iResult.getOverlapping()).addOwners(oldSourceEntry));
                            destList.add(destIndex, oldDestEntry.replaceRegion(iResult.getANonOverlapping()));
                        } else {
                            throw new RuntimeException("panic");
                        }
                    } else if (iResult.getANonOverlapping().size() == 2) {
                        destList.add(destIndex, oldDestEntry.replaceRegion(iResult.getANonOverlapping().get(1)));
                        destList.add(destIndex, oldDestEntry.replaceRegion(iResult.getOverlapping()).addOwners(oldSourceEntry));
                        destList.add(destIndex, oldDestEntry.replaceRegion(iResult.getANonOverlapping().get(0)));
                    } else {
                        throw new RuntimeException("panic");
                    }
                    destScanActive = false; // go on to next source entry 
                    break;
                case bSubsumesA: // fall through
                    destList.remove(destIndex);
                    if (iResult.getBNonOverlapping().size() == 1) {
                        sourceList.add(0, oldSourceEntry.replaceRegion(iResult.getBNonOverlapping()));
                        destList.add(destIndex, oldDestEntry.replaceRegion(iResult.getOverlapping()).addOwners(oldSourceEntry));
                    } else if (iResult.getBNonOverlapping().size() == 2) {
                        sourceList.add(0, oldSourceEntry.replaceRegion(iResult.getBNonOverlapping().get(1)));
                        destList.add(destIndex, oldDestEntry.replaceRegion(iResult.getOverlapping()).addOwners(oldSourceEntry));
                        sourceList.add(0, oldSourceEntry.replaceRegion(iResult.getBNonOverlapping().get(0)));
                    } else {
                        throw new RuntimeException("panic");
                    }
                    destScanActive = false; // go on to next source entry 
                    break;
                case wrappedPartial:
                		// below is from abPartial
                    destList.remove(destIndex);
                    destList.add(destIndex, oldDestEntry.replaceRegion(iResult.getOverlapping().get(1)).addOwners(oldSourceEntry));
                    destList.add(destIndex, oldDestEntry.replaceRegion(iResult.getANonOverlapping()));
                    destList.add(destIndex, oldDestEntry.replaceRegion(iResult.getOverlapping().get(0)).addOwners(oldSourceEntry));
                    sourceList.add(0, oldSourceEntry.replaceRegion(iResult.getBNonOverlapping()));
                    destScanActive = false; // go on to next source entry 
                	break;
                case nonIdenticalAllRingspace:
                	// This case is prevented by the fact RingRegion normalizes all all-ringspace regions
                    throw new RuntimeException("panic");
                default:
                    throw new RuntimeException("panic");
                }
            }
            if (destScanActive) { // if we didn't add it, then add here
            	destList.add(insertionIndex, oldSourceEntry);
                //destList.add(oldSourceEntry);
                //Collections.sort(destList, RingEntryPositionComparator.instance);
            }
        }
        RingEntry.ensureEntryRegionsDisjoint(destList);
        if (debug) {
            System.out.println("merge complete: **************");
            System.out.println("\t\t"+ RingEntry.toString(destList));
            System.out.println("======================================");
            System.out.println();
        }
    }

    private List<RingEntry> getRawEntryList(Node node) {
        List<RingEntry> entryList;
        
        entryList = new ArrayList<>();
        TopologyRing    topoRing;
        
        topoRing = maps.get(node.getIDString());
        if (topoRing == null) {
            System.err.println("Known nodes:");
            for (String id : maps.keySet()) {
                System.err.println(id);
            }
            throw new RuntimeException("Can't find ring for "+ node.getIDString());
        }
        for (RingEntry ringEntry : topoRing.getMembers()) {
            entryList.add(ringEntry);
        }
        return entryList;
    }
    
    private List<RingEntry> projectEntryList(List<RingEntry> entryList, RingRegion parent) {
        RingRegion  projectedRegion;
        List<RingEntry> projectedList;
        
        if (debug) {
            System.out.println("projectEntryList "+ parent);
            System.out.println(RingEntry.toString(entryList));
            System.out.println();
        }
        projectedList = new ArrayList<>();
        for (RingEntry entry : entryList) {
            projectedRegion = LongRingspace.mapChildRegionToParentRegion(LongRingspace.globalRegion, entry.getRegion(), parent);
            projectedList.add(entry.replaceRegion(projectedRegion));
        }
        if (debug) {
            System.out.println("out projectEntryList "+ parent);
            System.out.println(RingEntry.toString(projectedList));
            System.out.println();
        }
        return projectedList;
    }
    
    private List<RingEntry> cleanupList(RingRegion ringspace, List<RingEntry> dirty) {
        if (Math.abs(RingRegion.getTotalSize(RingEntry.getRegions(dirty)) - ringspace.getSize()) > allowedError) {
            System.err.println("ringspace");
            System.err.println(ringspace);
            System.err.println("RingEntry.toString(dirty)");
            System.err.println(RingEntry.toString(dirty));
            System.err.printf("RingRegion.getTotalSize(RingEntry.getRegions(dirty)) %d\n", 
                    RingRegion.getTotalSize(RingEntry.getRegions(dirty)));
            System.err.printf("ringspace.getSize() %d\n", ringspace.getSize());
            throw new RuntimeException("Size error exceeds allowed limit");
        } else {
            List<RingEntry> clean;
            
            clean = new ArrayList<>(dirty.size());
            if (dirty.size() == 0) {
                // no action required
            } else if (dirty.size() == 1) {
                clean.add(dirty.get(0).replaceRegion(ringspace));
            } else {
                for (int i = 0; i < dirty.size() - 1; i++) {
                    RingRegion  r1;
                    RingRegion  r2;
                    long        error;
                    int         j;
                    
                    j = (i + 1) % dirty.size();
                    r1 = dirty.get(i).getRegion();
                    r2 = dirty.get(j).getRegion();                    
                    error = Math.abs(LongRingspace.nextPoint(r1.getEnd()) - r2.getStart());
                    if (error != 0) {
                        if (error > allowedContiquityError) {
                            System.err.println(r1);
                            System.err.println(r2);
                            throw new RuntimeException("Contiguity error exceeds allowed limit");
                        } else {
                            clean.add(dirty.get(i).replaceRegion(new RingRegion(r1.getStart(), LongRingspace.prevPoint(r2.getStart()))));
                            /*
                            if (r1.getSize() >= r2.getSize()) {
                                clean.add(dirty.get(i).replaceRegion(new RingRegion(r1.getStart(), LongRingspace.prevPoint(r2.getStart()))));
                            } else {
                                //clean.add(dirty.get(j).replaceRegion(new RingRegion(LongRingspace.nextPoint(r1.getEnd()), r2.getEnd())));
                            }
                            */
                        }
                    } else {
                        clean.add(dirty.get(i));
                    }
                }
                clean.add(dirty.get(dirty.size() - 1));
            }
            return RingEntry.simplify(clean);
        }
    }
    
    //////////////////////////////////
    
    public Collection<Node> getMemberNodes(OwnerQueryMode oqm) {
        Set<Node>   memberNodes;
        
        memberNodes = new HashSet<>();
        for (TopologyRing topoRing : maps.values()) {
            memberNodes.addAll(topoRing.getMemberNodes(oqm));
        }
        return memberNodes;
    }

    public Set<Node> getMemberNodes(OwnerQueryMode oqm, NodeClass nodeClass) {
        ImmutableSet.Builder<Node>	replicas;

        replicas = ImmutableSet.builder();
        for (Node node : getMemberNodes(oqm)) {
        	if (node.getNodeClass() == nodeClass) {
        		replicas.add(node);
        	}
        }
        return replicas.build();
    }
    
    //////////////////////////////////
    
    public void test(String pov) {
        Random  rand;
        
        testCoordinate(LongRingspace.start, pov);
        testCoordinate(LongRingspace.end, pov);
        testCoordinate(-1, pov);
        testCoordinate(0, pov);
        testCoordinate(1, pov);
        System.out.println();
        testCoordinate(LongRingspace.fractionToLong(0), pov);
        testCoordinate(LongRingspace.fractionToLong(0.24), pov);
        testCoordinate(LongRingspace.fractionToLong(0.26), pov);
        testCoordinate(LongRingspace.fractionToLong(0.49), pov);
        System.out.println();
        testCoordinate(LongRingspace.fractionToLong(0.51), pov);
        testCoordinate(LongRingspace.fractionToLong(0.74), pov);
        testCoordinate(LongRingspace.fractionToLong(0.76), pov);
        testCoordinate(LongRingspace.fractionToLong(0.99), pov);
        System.out.println();
        rand = new Random();
        for (int i = 0; i < 5; i++) {
            testCoordinate(LongRingspace.longToRingspace(rand.nextLong()), pov);
        }
    }
    
    public void testDistance(String id1, String id2) {
        System.out.println(id1 +" "+ id2 +" "+ topology.getDistanceByID(id1, id2));
    }
    
    public void testCoordinate(long coordinate, String pov) {
        System.out.print(coordinate +"\t");
        for (Node node : getStorageNodesOrdered(coordinate, pov)) {
            System.out.print(node +" ");
        }
        System.out.println();
    }
    
    @Override
    public String toString() {
        StringBuilder   sb;
        
        sb = new StringBuilder();
        sb.append(topology.toString());
        sb.append("\n\n");
        for (Map.Entry<String,TopologyRing> entry : maps.entrySet()) {
            sb.append(entry.getKey());
            sb.append('\n');
            sb.append(entry.getValue());
            sb.append("\n\n");
        }
        return sb.toString();
    }
    
    private class NodeDistanceComparator implements Comparator<Node> {
        private final String  pov;
        
        private NodeDistanceComparator(String pov) {
            this.pov = pov;
        }
        
        @Override
        public int compare(Node n1, Node n2) {
            int d1;
            int d2;
            
            d1 = topology.getDistanceByID(pov, n1.getIDString());
            d2 = topology.getDistanceByID(pov, n2.getIDString());
            if (d1 < d2) {
                return -1;
            } else if (d1 > d2) {
                return 1;
            } else {
                return 0;
            }
        }        
    }
}
