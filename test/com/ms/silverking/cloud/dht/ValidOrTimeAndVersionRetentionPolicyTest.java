package com.ms.silverking.cloud.dht;

import static com.ms.silverking.cloud.dht.TestUtil.goCopy;
import static com.ms.silverking.cloud.dht.TestUtil.goDiff;
import static com.ms.silverking.cloud.dht.TestUtil.ioCopy;
import static com.ms.silverking.cloud.dht.TestUtil.ioDiff;
import static com.ms.silverking.cloud.dht.TestUtil.poCopy;
import static com.ms.silverking.cloud.dht.TestUtil.poDiff;
import static com.ms.silverking.cloud.dht.TestUtil.woCopy;
import static com.ms.silverking.cloud.dht.TestUtil.woDiff;
import static com.ms.silverking.testing.AssertFunction.checkHashCodeEquals;
import static com.ms.silverking.testing.AssertFunction.checkHashCodeNotEquals;
import static com.ms.silverking.testing.AssertFunction.test_Equals;
import static com.ms.silverking.testing.AssertFunction.test_FirstEqualsSecond_FirstNotEqualsThird;
import static com.ms.silverking.testing.AssertFunction.test_NotEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.ms.silverking.cloud.dht.ValidOrTimeAndVersionRetentionPolicy.Mode;

public class ValidOrTimeAndVersionRetentionPolicyTest {

	private static final ValidOrTimeAndVersionRetentionPolicy defaultPolicy     = ValidOrTimeAndVersionRetentionPolicy.template;
	private static final ValidOrTimeAndVersionRetentionPolicy defaultPolicyCopy = new ValidOrTimeAndVersionRetentionPolicy(Mode.wallClock, 1, 86400);
	private static final ValidOrTimeAndVersionRetentionPolicy defaultPolicyDiff = new ValidOrTimeAndVersionRetentionPolicy(Mode.wallClock, 1, 86401);
	
	@Test
	public void testGetters() {
		Object[][] testCases = {
			{defaultPolicy,     Mode.wallClock, 1, 86400L},
			{defaultPolicyDiff, Mode.wallClock, 1, 86401L},
		};
		
		for (Object[] testCase : testCases) {
			ValidOrTimeAndVersionRetentionPolicy policy = (ValidOrTimeAndVersionRetentionPolicy)testCase[0];
			Mode expectedMode                           =                                 (Mode)testCase[1];
			int expectedMinVersions                     =                                  (int)testCase[2];
			long expectedTimeSpanSeconds                =                                 (long)testCase[3];

			assertEquals(expectedMode,                          policy.getMode());
			assertEquals(expectedMinVersions,                   policy.getMinVersions());
			assertEquals(expectedTimeSpanSeconds,               policy.getTimeSpanSeconds());
			assertEquals(expectedTimeSpanSeconds*1_000,         policy.getTimeSpanMillis());
			assertEquals(expectedTimeSpanSeconds*1_000_000_000, policy.getTimeSpanNanos());
		}
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
