package com.ms.silverking.cloud.dht;

import static com.ms.silverking.cloud.dht.TestUtil.getImplementationType;
import static com.ms.silverking.cloud.dht.ValueRetentionPolicy.ImplementationType.SingleReverseSegmentWalk;
import static com.ms.silverking.testing.AssertFunction.checkHashCodeEquals;
import static com.ms.silverking.testing.AssertFunction.checkHashCodeNotEquals;
import static com.ms.silverking.testing.AssertFunction.test_FirstEqualsSecond_FirstNotEqualsThird;
import static com.ms.silverking.testing.AssertFunction.test_Getters;
import static com.ms.silverking.testing.AssertFunction.test_NotEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.ms.silverking.cloud.dht.ValidOrTimeAndVersionRetentionPolicy.Mode;
import com.ms.silverking.cloud.dht.common.DHTKey;

public class ValidOrTimeAndVersionRetentionPolicyTest {

	private static final Mode mCopy = Mode.wallClock;
	private static final Mode mDiff = Mode.mostRecentValue;
	
	private static final int mvCopy = 1;
	private static final int mvDiff = 0;
	
	private static final long tssCopy = 86_400;
	private static final long tssDiff = 86_401;
	
	private static final ValidOrTimeAndVersionRetentionPolicy defaultPolicy           =     ValidOrTimeAndVersionRetentionPolicy.template;
	private static final ValidOrTimeAndVersionRetentionPolicy defaultPolicyCopy       = new ValidOrTimeAndVersionRetentionPolicy(mCopy, mvCopy, tssCopy);
//	private static final ValidOrTimeAndVersionRetentionPolicy defaultPolicyAlmostCopy = new ValidOrTimeAndVersionRetentionPolicy(mCopy, mvCopy, tssDiff);
	private static final ValidOrTimeAndVersionRetentionPolicy defaultPolicyDiff       = new ValidOrTimeAndVersionRetentionPolicy(mDiff, mvDiff, tssDiff);
	
	@Test
	public void testGetters() {
		Object[][] testCases = {
			{SingleReverseSegmentWalk,                   getImplementationType(defaultPolicy)},
//			{new ValidOrTimeAndVersionRetentionPolicy(), getInitialState(defaultPolicy)},	// FIXME:bph: currently no equals on ValidOrTimeAndVersionRetentionPolicy so this will assert !equal
		};
		
		test_Getters(testCases);
			
		Object[][] testCases2 = {
			{defaultPolicy,     mCopy, mvCopy, tssCopy},
			{defaultPolicyDiff, mDiff, mvDiff, tssDiff},
		};
		
		for (Object[] testCase : testCases2) {
			ValidOrTimeAndVersionRetentionPolicy policy = (ValidOrTimeAndVersionRetentionPolicy)testCase[0];
			Mode expectedMode                           =                                 (Mode)testCase[1];
			int  expectedMinVersions                    =                                  (int)testCase[2];
			long expectedTimeSpanSeconds                =                                 (long)testCase[3];

			assertEquals(expectedMode,                          policy.getMode());
			assertEquals(expectedMinVersions,                   policy.getMinVersions());
			assertEquals(expectedTimeSpanSeconds,               policy.getTimeSpanSeconds());
			assertEquals(expectedTimeSpanSeconds*1_000,         policy.getTimeSpanMillis());
			assertEquals(expectedTimeSpanSeconds*1_000_000_000, policy.getTimeSpanNanos());
		}
	}

	@Test
	public void testRetains() {
		DHTKey key = null;
		Object[][] testCases = {
			{defaultPolicy,     key, 0L,   0L, false, new TimeAndVersionRetentionState(), 0L, true},	// invalidated condition
			{defaultPolicy,     key, 0L,   0L, true,  new TimeAndVersionRetentionState(), 0L, true},	// minVersion  condition
			{defaultPolicyDiff, key, 0L, 100L, true,  new TimeAndVersionRetentionState(), 0L, true},	// delta       condition
		};
		
		TestUtil.checkRetains(testCases);
		
		TimeAndVersionRetentionState state = new TimeAndVersionRetentionState();
		state.processValue(key, defaultPolicyDiff.getTimeSpanSeconds()*1_000_000_000+1);
		Object[][] testCasesFalse = {
			{defaultPolicyDiff, key, 0L, 0L, true, state, 0L, false},	
		};
		
		TestUtil.checkRetains(testCasesFalse);
	}

	@Test
	public void testHashCode() {
		checkHashCodeEquals(   defaultPolicy, defaultPolicy);
		checkHashCodeEquals(   defaultPolicy, defaultPolicyCopy);
		checkHashCodeNotEquals(defaultPolicy, defaultPolicyDiff);
	}
	
	@Test
	public void testEqualsObject() {
		ValidOrTimeAndVersionRetentionPolicy[][] testCases = {
			{defaultPolicy,     defaultPolicy,     defaultPolicyDiff},
			{defaultPolicyCopy, defaultPolicy,     defaultPolicyDiff},
			{defaultPolicyDiff, defaultPolicyDiff, defaultPolicy},
		};
		test_FirstEqualsSecond_FirstNotEqualsThird(testCases);

		test_NotEquals(new Object[][]{
			{defaultPolicy,    InvalidatedRetentionPolicy.template},
			{defaultPolicy, TimeAndVersionRetentionPolicy.template},
		});
	}

	@Test
	public void testToStringAndParse() {
		ValidOrTimeAndVersionRetentionPolicy[] testCases = {
			defaultPolicy,
			defaultPolicyDiff,
		};
		
		for (ValidOrTimeAndVersionRetentionPolicy testCase : testCases)
			checkStringAndParse(testCase);
	}
	
	private void checkStringAndParse(ValidOrTimeAndVersionRetentionPolicy policy) {
		assertEquals(policy, ValidOrTimeAndVersionRetentionPolicy.parse( policy.toString() ));
	}

}
