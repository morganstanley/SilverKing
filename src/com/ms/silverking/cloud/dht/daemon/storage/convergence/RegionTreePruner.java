package com.ms.silverking.cloud.dht.daemon.storage.convergence;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import com.ms.silverking.cloud.dht.common.KeyUtil;
import com.ms.silverking.cloud.dht.daemon.storage.KeyAndVersionChecksum;
import com.ms.silverking.cloud.ring.IntersectionResult;
import com.ms.silverking.cloud.ring.IntersectionType;
import com.ms.silverking.cloud.ring.RingRegion;
import com.ms.silverking.log.Log;

/**
 * Prunes a complete region tree to create a new tree pertaining
 * to a sub region.
 */
public class RegionTreePruner {
	public static ChecksumNode prune(ChecksumNode root, RingRegion pruneRegion) {
		if (Log.levelMet(Level.INFO)) {
			Log.info("in prune "+ root.getRegion() +" "+ pruneRegion +" "+ root.estimatedKeys());
		}
		if (root.getRegion().equals(pruneRegion)) {
			Log.info("root.getRegion == pruneRegion");
			return root;
		} else {
			if (RingRegion.intersectionType(root.getRegion(), pruneRegion) != IntersectionType.aSubsumesB) {
				throw new RuntimeException("Invalid pruneRegion");
			} else {
				ChecksumNode	prunedRoot;
				
				prunedRoot = _prune(root, pruneRegion);
				if (Log.levelMet(Level.INFO)) {
					Log.info("out prune "+ prunedRoot.getRegion() +" "+ pruneRegion +" "+ prunedRoot.estimatedKeys());
				}
				return prunedRoot;
			}
		}
	}
	
	private static ChecksumNode _prune(ChecksumNode node, RingRegion pruneRegion) {
		if (node instanceof LeafChecksumNode) {
			return pruneLeafNode((LeafChecksumNode)node, pruneRegion);
		} else if (node instanceof NonLeafChecksumNode) {
			return pruneNonLeafNode((NonLeafChecksumNode)node, pruneRegion);
		} else {
			throw new RuntimeException("panic");
		}
	}
	
	private static ChecksumNode pruneNonLeafNode(NonLeafChecksumNode node, RingRegion pruneRegion) {
		List<ChecksumNode>	newChildren;
		IntersectionResult	iResult;
		
		if (!node.getRegion().overlaps(pruneRegion)) {
			throw new RuntimeException("panic");
		}
		
		iResult = RingRegion.intersect(node.getRegion(), pruneRegion);
		if (iResult.getIntersectionType() == IntersectionType.wrappedPartial) {
			// should be a very unusual case
			// FUTURE - handle this
			throw new RuntimeException("wrappedPartial pruning not yet handled");
		}
		
		newChildren = new ArrayList<>();
		for (ChecksumNode child : node.getChildren()) {
			if (child.getRegion().overlaps(pruneRegion)) {
				newChildren.add(_prune(child, pruneRegion));
			}
		}
		
		return new NonLeafChecksumNode(iResult.getOverlapping().get(0), newChildren);
	}
	
	private static ChecksumNode pruneLeafNode(LeafChecksumNode node, RingRegion pruneRegion) {
		IntersectionResult	iResult;
		RingRegion			oRegion;
		//long[]				_kvc;
		List<KeyAndVersionChecksum>	kvcList;
		List<KeyAndVersionChecksum>	prunedKVCList;
		
		if (!node.getRegion().overlaps(pruneRegion)) {
			throw new RuntimeException("panic");
		}
		
		iResult = RingRegion.intersect(node.getRegion(), pruneRegion);
		if (iResult.getIntersectionType() == IntersectionType.wrappedPartial) {
			// should be a very unusual case
			// FUTURE - handle this
			throw new RuntimeException("wrappedPartial pruning not yet handled");
		}
		
		oRegion = iResult.getOverlapping().get(0);
		
		//_kvc = node.getKeyAndVersionChecksumsAsArray();
		
		// FUTURE - improve the efficiency of this approach
		// This is n log n, and each step is heavy
		// Should be able to do at least log n with very light steps
		kvcList = node.getKeyAndVersionChecksums();
		prunedKVCList = new ArrayList<>(kvcList.size());
		for (KeyAndVersionChecksum kvc : kvcList) {
			if (pruneRegion.contains(KeyUtil.keyToCoordinate(kvc.getKey()))) {
				prunedKVCList.add(kvc);
			}
		}
		
		return new LeafChecksumNode(oRegion, prunedKVCList);
	}	
}
