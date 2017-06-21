package com.ms.silverking.cloud.dht.client;

import static com.ms.silverking.cloud.dht.client.SimpleTimeoutController.defaultMaxAttempts;
import static com.ms.silverking.cloud.dht.client.SimpleTimeoutController.defaultMaxRelativeTimeoutMillis;
import static com.ms.silverking.cloud.dht.client.TestUtil.*;
import static com.ms.silverking.testing.AssertFunction.*;
import static com.ms.silverking.testing.AssertFunction.test_NotEquals;
import static com.ms.silverking.testing.AssertFunction.test_FirstEqualsSecond_SecondNotEqualsThird;
import static com.ms.silverking.testing.AssertFunction.test_Getters;
import static com.ms.silverking.testing.AssertFunction.test_SetterExceptions;
import static com.ms.silverking.testing.AssertFunction.test_Setters;
import static com.ms.silverking.testing.Util.*;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.ms.silverking.testing.Util.ExceptionChecker;

public class SimpleTimeoutControllerTest {

	private static final int maCopy = 5;
	private static final int maDiff = 6;
	
	private static final int mrtomCopy = 120_000;
	private static final int mrtomDiff = 119_999;
	
	private static final SimpleTimeoutController defaultController     =     SimpleTimeoutController.template;
	private static final SimpleTimeoutController defaultControllerCopy = new SimpleTimeoutController(maCopy, mrtomCopy);
	private static final SimpleTimeoutController defaultControllerDiff = new SimpleTimeoutController(maDiff, mrtomDiff);
	
	private SimpleTimeoutController setMaxAttempts(int ma) {
		return defaultController.maxAttempts(ma);
	}
	
	private SimpleTimeoutController setMaxRelTimeoutMillis(int mrtom) {
		return defaultController.maxRelativeTimeoutMillis(mrtom);
	}
	
	@Test
	public void testGetters() {
		Object[][] testCases = {
			{defaultMaxAttempts,              getMaxAttempts(defaultController)},
			{int_maxVal,                      getRelativeTimeout_Null(defaultController)},
			{defaultMaxRelativeTimeoutMillis, getMaxRelativeTimeout_Null(defaultController)},
		};
		
		test_Getters(testCases);
	}

	// this takes care of testing ctors as well
	@Test
	public void testSetters_Exceptions() {
		Object[][] testCases = {
			{"maxAttempts < min_maxAttempts", new ExceptionChecker() { @Override public void check() { setMaxAttempts(0); } }, RuntimeException.class},
		};

		test_SetterExceptions(testCases);
	}
	
	@Test
	public void testSetters() {
		SimpleTimeoutController    maController = setMaxAttempts(maDiff);
		SimpleTimeoutController mrtomController = setMaxRelTimeoutMillis(mrtomDiff);

		Object[][] testCases = {
			{maDiff,     getMaxAttempts(maController)},
			{int_maxVal, getRelativeTimeout_Null(mrtomController)},
			{mrtomDiff,  getMaxRelativeTimeout_Null(mrtomController)},
		};
		
		test_Setters(testCases);
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
			{defaultController,     defaultController,                 defaultControllerDiff},
			{defaultControllerDiff, defaultControllerDiff,             defaultController},
			{defaultControllerCopy, defaultController,                 defaultControllerDiff},
			{defaultController,     setMaxAttempts(maCopy),            setMaxAttempts(maDiff)},
			{defaultController,     setMaxRelTimeoutMillis(mrtomCopy), setMaxRelTimeoutMillis(mrtomDiff)},
		};
		
		test_FirstEqualsSecond_SecondNotEqualsThird(testCases);
		test_NotEquals(new Object[][]{
			{defaultController, OpSizeBasedTimeoutController.template},
			{defaultController,     WaitForTimeoutController.template},
		});
	}

	@Test
	public void testToStringAndParse() {
		SimpleTimeoutController[] testCases = {
			defaultController,
			defaultControllerCopy,
			defaultControllerDiff,
		};
		
		for (SimpleTimeoutController testCase : testCases)
			checkStringAndParse(testCase);
	}
	
	private void checkStringAndParse(SimpleTimeoutController controller) {
		assertEquals(controller, SimpleTimeoutController.parse( controller.toString() ));
	}
}
