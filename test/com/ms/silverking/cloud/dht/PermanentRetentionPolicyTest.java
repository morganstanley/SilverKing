package com.ms.silverking.cloud.dht;

import static com.ms.silverking.cloud.dht.TestUtil.getImplementationType;
import static com.ms.silverking.testing.AssertFunction.*;
import static org.junit.Assert.*;

import org.junit.Test;

import com.ms.silverking.cloud.dht.ValueRetentionPolicy.ImplementationType;

public class PermanentRetentionPolicyTest {

	private static final PermanentRetentionPolicy defaultPolicy = PermanentRetentionPolicy.template;

	// this takes care of testing ctors as well
	@Test
	public void testGetters() {
		Object[][] testCases = {
			{ImplementationType.RetainAll, getImplementationType(defaultPolicy)},
		};
		
		test_Getters(testCases);
	}

	@Test
	public void testHashCode() {
		checkHashCodeEquals(defaultPolicy, defaultPolicy);
	}
	
	@Test
	public void testEqualsObject() {
		test_EqualsOrNotEquals(new Object[][]{
			{defaultPolicy,                          defaultPolicy,  true},
			{defaultPolicy,    InvalidatedRetentionPolicy.template, false},
			{defaultPolicy, TimeAndVersionRetentionPolicy.template, false},
		});
	}

	@Test
	public void testToStringAndParse() {
		PermanentRetentionPolicy[] testCases = {
			defaultPolicy,
		};
		
		for (PermanentRetentionPolicy testCase : testCases)
			checkStringAndParse(testCase);
	}
	
	private void checkStringAndParse(PermanentRetentionPolicy controller) {
		assertEquals(controller, PermanentRetentionPolicy.parse( controller.toString() ));
	}
}
