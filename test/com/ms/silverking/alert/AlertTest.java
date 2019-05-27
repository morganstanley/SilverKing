package com.ms.silverking.alert;

import static com.ms.silverking.testing.AssertFunction.checkHashCodeEquals;
import static com.ms.silverking.testing.AssertFunction.checkHashCodeNotEquals;
import static com.ms.silverking.testing.AssertFunction.test_FirstEqualsSecond_FirstNotEqualsThird;
import static com.ms.silverking.testing.AssertFunction.test_Getters;
import static com.ms.silverking.testing.AssertFunction.test_SetterExceptions;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.ms.silverking.testing.Util.ExceptionChecker;

public class AlertTest {

	private static final String context = "context";
	private static final int level      = 1;
	private static final String key     = "key";
	private static final String message = "message";
	private static final String data    = "data";
	
	static final Alert alert                      = new Alert(context, level, key, message, data);
	private static final Alert alertCopy          = new Alert(context, level, key, message, data);
	private static final Alert alertDiff_NullData = new Alert(context, level, key, message, null);
	
	static final String alertTestExpectedToString = context+":"+level+":"+key+":"+message;
	
	@Test
	public void testGetters() {
		Object[][] testCases = {
			{context, alert.getContext()},
			{level,   alert.getLevel()},
			{key,     alert.getKey()},
			{message, alert.getMessage()},
			{data,    alert.getData()},
			{"",      alertDiff_NullData.getData()},
		};
		
		test_Getters(testCases);
	}
	
	@Test
	public void testConstructor_NullExceptions() {
		Object[][] testCases = {
			{"context = null", new ExceptionChecker() { @Override public void check() { new Alert(null, 0, null, null, null); } }, NullPointerException.class},
			{"key = null",     new ExceptionChecker() { @Override public void check() { new Alert("",   0, null, null, null); } }, NullPointerException.class},
			{"message = null", new ExceptionChecker() { @Override public void check() { new Alert("",   0,   "", null, null); } }, NullPointerException.class},
		};

		test_SetterExceptions(testCases);
	}
		
	@Test
	public void testHashCode() {
		checkHashCodeEquals(   alert, alert);
		checkHashCodeEquals(   alert, alertCopy);
		checkHashCodeNotEquals(alert, alertDiff_NullData);
	}
	
	@Test
	public void testEqualsObject() {
		Alert[][] testCases = {
			{alert,              alert,              alertDiff_NullData},
			{alertCopy,          alert,              alertDiff_NullData},
			{alertDiff_NullData, alertDiff_NullData, alert},
		};
		test_FirstEqualsSecond_FirstNotEqualsThird(testCases);
		
		// FIXME:bph: equals doesn't check for instanceof right now
//		test_NotEquals(new Object[][]{
//			{alert, AlertReceiver.class},
//		});
	}

	@Test
	public void testToString() {
		Object[][] testCases = {
			{alertTestExpectedToString, alert},
			{alertTestExpectedToString, alertDiff_NullData},
		};
		
		for (Object[] testCase : testCases) {
			String expected = (String)testCase[0];
			Alert a         = (Alert)testCase[1];
			
			assertEquals(expected, a.toString());
		}
	}

}
