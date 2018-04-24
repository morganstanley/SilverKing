package com.ms.silverking.cloud.dht;

import java.util.HashMap;
import java.util.Map;

import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.collection.Pair;

public class TimeAndVersionRetentionState implements ValueRetentionState {
	private final Map<DHTKey,Pair<Integer,Long>>	vData;
	// We store the number of values and the most recent creation time
	
	@OmitGeneration
	public TimeAndVersionRetentionState() {
		vData = new HashMap<>();
	}

	// add a reference, store the creation time if not stored
	// return the number of references and the most recent creation time
	// Note: we carry backward the most recent creation time
	public Pair<Integer,Long> processValue(DHTKey key, long creationTime) {
		Pair<Integer,Long>	curVData;
		
		curVData = vData.get(key);
		if (curVData == null) {
			curVData = new Pair<>(0, creationTime);
		}
		curVData = new Pair<>(curVData.getV1() + 1, curVData.getV2());
		vData.put(key, curVData);
		return curVData;
	}
}
