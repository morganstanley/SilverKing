package com.ms.silverking.cloud.dht;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.KeyAndInteger;
import com.ms.silverking.collection.Pair;

public class TimeAndVersionRetentionStateTest {

	@Test
	public void testProcessValue() {
		TimeAndVersionRetentionState state = new TimeAndVersionRetentionState();
		DHTKey key = new KeyAndInteger(0, 1, 2);

		checkProcessValue(state, key, 3, new Pair<Integer, Long>(1, 3L));
		checkProcessValue(state, key, 4, new Pair<Integer, Long>(2, 3L));
	}
	
	private void checkProcessValue(TimeAndVersionRetentionState state, DHTKey key, long creationTime, Pair<Integer, Long> expected) {
		assertEquals(expected, state.processValue(key, creationTime));
	}

}
