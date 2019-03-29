package com.ms.silverking.cloud.dht;

import static com.ms.silverking.cloud.dht.TestUtil.getImplementationType;
import static com.ms.silverking.cloud.dht.ValueRetentionPolicy.ImplementationType.SingleReverseSegmentWalk;
import static com.ms.silverking.testing.AssertFunction.checkHashCodeEquals;
import static com.ms.silverking.testing.AssertFunction.checkHashCodeNotEquals;
import static com.ms.silverking.testing.AssertFunction.test_FirstEqualsSecond_FirstNotEqualsThird;
import static com.ms.silverking.testing.AssertFunction.test_Getters;
import static com.ms.silverking.testing.AssertFunction.test_NotEquals;
import static org.junit.Assert.*;

import org.junit.Test;

import com.ms.silverking.cloud.dht.TimeAndVersionRetentionPolicy.Mode;
import com.ms.silverking.cloud.dht.common.DHTKey;

public class NanosVersionRetentionPolicyTest {

	private static final int irisCopy = 0;
	private static final int irisDiff = 1;
	
	private static final long mrisCopy = 0;
	private static final long mrisDiff = 1;
	
	private static final NanosVersionRetentionPolicy defaultPolicy           =     NanosVersionRetentionPolicy.template;
	private static final NanosVersionRetentionPolicy defaultPolicyCopy       = new NanosVersionRetentionPolicy(irisCopy, mrisCopy);
	private static final NanosVersionRetentionPolicy defaultPolicyAlmostCopy = new NanosVersionRetentionPolicy(irisCopy, mrisDiff);
	private static final NanosVersionRetentionPolicy defaultPolicyDiff       = new NanosVersionRetentionPolicy(irisDiff, mrisDiff);
	
	@Test
	public void testGetters() {
		Object[][] testCases = {
			{SingleReverseSegmentWalk,        getImplementationType(defaultPolicy)},
//			{new InvalidatedRetentionState(), getInitialState(defaultPolicy)},	// FIXME:bph: currently no equals on InvalidatedRetentionState so this will assert !equal
		};
		
		test_Getters(testCases);
		
		// doesn't have other getters available.. getInvalidatedRetentionIntervalSeconds() or getMaxRetentionIntervalSeconds()
	}

	@Test
	public void testRetains() {
		Object[][] testCases = {
			{defaultPolicy,     null, 0L, 0L, false, new InvalidatedRetentionState(),                 0L,  true},
			{defaultPolicyDiff, null, 0L, 0L, false, new InvalidatedRetentionState(),                 0L,  true},	
			{defaultPolicyDiff, null, 0L, 0L, false, new InvalidatedRetentionState(), 1_000_000_000_000L, false},	
			{defaultPolicyDiff, null, 0L, 0L, true,  new InvalidatedRetentionState(),                 0L,  true},
			{defaultPolicyDiff, null, 0L, 0L, true,  new InvalidatedRetentionState(), 1_000_000_000_000L, false},
		};
		
		TestUtil.checkRetains(testCases);
	}

	@Test
	public void testHashCode() {
		checkHashCodeEquals(   defaultPolicy, defaultPolicy);
		checkHashCodeEquals(   defaultPolicy, defaultPolicyCopy);
		checkHashCodeEquals(   defaultPolicy, defaultPolicyDiff);	// this is b/c of Long.hashCode uses "^" operator
		checkHashCodeNotEquals(defaultPolicy, defaultPolicyAlmostCopy);	
	}
	
	@Test
	public void testEqualsObject() {
		NanosVersionRetentionPolicy[][] testCases = {
			{defaultPolicy,           defaultPolicy,           defaultPolicyDiff},
			{defaultPolicyCopy,       defaultPolicy,           defaultPolicyDiff},
			{defaultPolicyAlmostCopy, defaultPolicyAlmostCopy, defaultPolicy},
			{defaultPolicyDiff,       defaultPolicyDiff,       defaultPolicy},
		};
		test_FirstEqualsSecond_FirstNotEqualsThird(testCases);
		
		test_NotEquals(new Object[][]{
			{defaultPolicy, InvalidatedRetentionPolicy.template},
			{defaultPolicy,   PermanentRetentionPolicy.template},
		});
	}

	@Test
	public void testToStringAndParse() {
		NanosVersionRetentionPolicy[] testCases = {
			defaultPolicy,
			defaultPolicyCopy,
			defaultPolicyAlmostCopy,
			defaultPolicyDiff,
		};
		
		for (NanosVersionRetentionPolicy testCase : testCases)
			checkStringAndParse(testCase);
	}
	
	private void checkStringAndParse(NanosVersionRetentionPolicy policy) {
		assertEquals(policy, NanosVersionRetentionPolicy.parse( policy.toString() ));
	}

}
