package com.ms.silverking.cloud.dht.client;

import static com.ms.silverking.cloud.dht.client.TestUtil.getMaxAttempts;
import static com.ms.silverking.cloud.dht.client.TestUtil.getRelativeTimeout_Null;
import static com.ms.silverking.cloud.dht.client.WaitForTimeoutController.defaultInternalRetryIntervalSeconds;
import static com.ms.silverking.testing.AssertFunction.*;
import static com.ms.silverking.testing.Util.int_maxVal;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class WaitForTimeoutControllerTest {

	private static final int irisCopy = 20;
	private static final int irisDiff = 21;
	
	private static final WaitForTimeoutController defaultController     =     WaitForTimeoutController.template;
	private static final WaitForTimeoutController defaultControllerCopy = new WaitForTimeoutController(irisCopy);
	private static final WaitForTimeoutController defaultControllerDiff = new WaitForTimeoutController(irisDiff);

	// this takes care of testing ctors as well
	@Test
	public void testGetters() {
		Object[][] testCases = {
			{int_maxVal,                          getMaxAttempts(defaultController)},
			{defaultInternalRetryIntervalSeconds, getRelativeTimeout_Null(defaultController)},
//			{defaultMaxRelativeTimeoutMillis, getMaxRelativeTimeout_Null(defaultController)},	// NPE if AsyncOperation param is null, testing with null b/c it's too much work to create an actual AsynOperation...
			{int_maxVal,                          getMaxAttempts(defaultControllerDiff)},
			{irisDiff,                            getRelativeTimeout_Null(defaultControllerDiff)},
//			{defaultMaxRelativeTimeoutMillis, getMaxRelativeTimeout_Null(defaultControllerDiff)},	// NPE if AsyncOperation param is null, testing with null b/c it's too much work to create an actual AsynOperation...
		};
		
		test_Getters(testCases);
	}

	@Test
	public void testHashCode() {
		checkHashCodeEquals(   defaultController, defaultController);
		checkHashCodeEquals(   defaultController, defaultControllerCopy);
		checkHashCodeNotEquals(defaultController, defaultControllerDiff);
	}
	
	@Test
	public void testEqualsObject() {
		Object[][] testCases = {
			{defaultController,     defaultController,     defaultControllerDiff},
			{defaultControllerDiff, defaultControllerDiff, defaultController},
			{defaultControllerCopy, defaultController,     defaultControllerDiff},
		};
		
		test_FirstEqualsSecond_SecondNotEqualsThird(testCases);
		test_NotEquals(new Object[][]{
			{defaultController, OpSizeBasedTimeoutController.template},
			{defaultController,      SimpleTimeoutController.template},
		});
	}

	@Test
	public void testToStringAndParse() {
		WaitForTimeoutController[] testCases = {
			defaultController,
			defaultControllerCopy,
			defaultControllerDiff,
		};
		
		for (WaitForTimeoutController testCase : testCases)
			checkStringAndParse(testCase);
	}
	
	private void checkStringAndParse(WaitForTimeoutController controller) {
		assertEquals(controller, WaitForTimeoutController.parse( controller.toString() ));
	}
}
