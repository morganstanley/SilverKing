package com.ms.silverking.cloud.dht;

import static com.ms.silverking.cloud.dht.TestUtil.getImplementationType;
import static com.ms.silverking.testing.AssertFunction.*;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.ms.silverking.cloud.dht.ValueRetentionPolicy.ImplementationType;

public class InvalidatedRetentionPolicyTest {

	private static final int irisCopy = 0;
	private static final int irisDiff = 1;
	
	private static final InvalidatedRetentionPolicy defaultPolicy     =     InvalidatedRetentionPolicy.template;
	private static final InvalidatedRetentionPolicy defaultPolicyCopy = new InvalidatedRetentionPolicy(irisCopy);
	private static final InvalidatedRetentionPolicy defaultPolicyDiff = new InvalidatedRetentionPolicy(irisDiff);

	// this takes care of testing ctors as well
	@Test
	public void testGetters() {
		Object[][] testCases = {
			{ImplementationType.SingleReverseSegmentWalk, getImplementationType(defaultPolicy)},
			{ImplementationType.SingleReverseSegmentWalk, getImplementationType(defaultPolicyDiff)},
		};
		
		test_Getters(testCases);
	}

	@Test
	public void testHashCode() {
		checkHashCodeEquals(   defaultPolicy, defaultPolicy);
		checkHashCodeEquals(   defaultPolicy, defaultPolicyCopy);
		checkHashCodeNotEquals(defaultPolicy, defaultPolicyDiff);
	}
	
	@Test
	public void testEqualsObject() {
		Object[][] testCases = {
			{defaultPolicy,     defaultPolicy,     defaultPolicyDiff},
			{defaultPolicyDiff, defaultPolicyDiff, defaultPolicy},
			{defaultPolicyCopy, defaultPolicy,     defaultPolicyDiff},
		};
		
		test_FirstEqualsSecond_SecondNotEqualsThird(testCases);
		test_NotEquals(new Object[][]{
			{defaultPolicy,      PermanentRetentionPolicy.template},
			{defaultPolicy, TimeAndVersionRetentionPolicy.template},
		});
	}

	@Test
	public void testToStringAndParse() {
		InvalidatedRetentionPolicy[] testCases = {
			defaultPolicy,
			defaultPolicyCopy,
			defaultPolicyDiff,
		};
		
		for (InvalidatedRetentionPolicy testCase : testCases)
			checkStringAndParse(testCase);
	}
	
	private void checkStringAndParse(InvalidatedRetentionPolicy controller) {
		assertEquals(controller, InvalidatedRetentionPolicy.parse( controller.toString() ));
	}
}
