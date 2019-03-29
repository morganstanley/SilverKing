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

import com.ms.silverking.cloud.dht.TimeAndVersionRetentionPolicy.Mode;
import com.ms.silverking.cloud.dht.common.DHTKey;

public class TimeAndVersionRetentionPolicyTest {

	private static final Mode mCopy = Mode.wallClock;
	private static final Mode mDiff = Mode.mostRecentValue;
	
	private static final int mvCopy = 1;
	private static final int mvDiff = 0;
	
	private static final long tssCopy = 86_400;
	private static final long tssDiff = 86_399;
	
	private static final TimeAndVersionRetentionPolicy defaultPolicy           =     TimeAndVersionRetentionPolicy.template;
	private static final TimeAndVersionRetentionPolicy defaultPolicyCopy       = new TimeAndVersionRetentionPolicy(mCopy, mvCopy, tssCopy);
	private static final TimeAndVersionRetentionPolicy defaultPolicyAlmostCopy = new TimeAndVersionRetentionPolicy(mCopy, mvCopy, tssDiff);
	private static final TimeAndVersionRetentionPolicy defaultPolicyDiff       = new TimeAndVersionRetentionPolicy(mDiff, mvDiff, tssDiff);
	
	@Test
	public void testGetters() {
		Object[][] testCases = {
			{SingleReverseSegmentWalk,           getImplementationType(defaultPolicy)},
//			{new TimeAndVersionRetentionState(), getInitialState(defaultPolicy)},	// FIXME:bph: currently no equals on TimeAndVersionRetentionState so this will assert !equal
		};
		
		test_Getters(testCases);
		
		Object[][] testCases2 = {
			{defaultPolicy,     mCopy, mvCopy, tssCopy},
			{defaultPolicyDiff, mDiff, mvDiff, tssDiff},
		};
		
		for (Object[] testCase : testCases2) {
			TimeAndVersionRetentionPolicy policy = (TimeAndVersionRetentionPolicy)testCase[0];
			Mode expectedMode                    =                          (Mode)testCase[1];
			int  expectedMinVersions             =                           (int)testCase[2];
			long expectedTimeSpanSeconds         =                          (long)testCase[3];

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
			{defaultPolicy,     key, 0L,   0L, false, new TimeAndVersionRetentionState(), 0L,  true},	// minVersion condition
			{defaultPolicyDiff, key, 0L, 100L, true,  new TimeAndVersionRetentionState(), 0L,  true},	// delta      condition
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
		checkHashCodeNotEquals(defaultPolicy, defaultPolicyAlmostCopy);
		checkHashCodeNotEquals(defaultPolicy, defaultPolicyDiff);
	}
	
	@Test
	public void testEqualsObject() {
		TimeAndVersionRetentionPolicy[][] testCases = {
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
		TimeAndVersionRetentionPolicy[] testCases = {
			defaultPolicy,
			defaultPolicyCopy,
			defaultPolicyAlmostCopy,
			defaultPolicyDiff,
		};
		
		for (TimeAndVersionRetentionPolicy testCase : testCases)
			checkStringAndParse(testCase);
	}
	
	private void checkStringAndParse(TimeAndVersionRetentionPolicy policy) {
		assertEquals(policy, TimeAndVersionRetentionPolicy.parse( policy.toString() ));
	}

}
