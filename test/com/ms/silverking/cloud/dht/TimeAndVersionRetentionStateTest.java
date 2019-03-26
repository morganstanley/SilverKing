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
		
		assertEquals(new Pair<Integer, Long>(1, 3L), state.processValue(key, 3));
		assertEquals(new Pair<Integer, Long>(2, 3L), state.processValue(key, 4));
	}

}
