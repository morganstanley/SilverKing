package com.ms.silverking.numeric;

import static com.ms.silverking.testing.Util.double_nan;
import static com.ms.silverking.testing.Util.getTestMessage;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class LogBaseNTest {

	@Test
	public void testLogBaseN() {
		double[][] testCases = {
			{0, 0, double_nan},
			{1, 1, double_nan},
			{2, 2, 	        1},
			{4_000_000_123d, 4_000_000_123d,     	         1},
			{4_000_000_000d, 2_000_000_000d, .9686494360651929},
			{2_000_000_000d, 4_000_000_000d, 1.032365232216681},
		};
			
		for (double[] testCase : testCases) {
			double base              = testCase[0];
			double x                 = testCase[1];
			double expectedLogResult = testCase[2];
			String expectedString = "log "+base;

			LogBaseN baseN = new LogBaseN(base);
			checkLog(baseN, x, expectedLogResult);
			checkToString(baseN, expectedString);
		}
	}
	
	private void checkLog(LogBaseN base, double x, double expected) {
		assertEquals( getTestMessage("getMostSignificantBits", "x = " + x),	expected, base.log(x), 0.0);
	}
	
	private void checkToString(LogBaseN baseN, String expected) {
		assertEquals( getTestMessage("toString"), expected, baseN.toString());
	}
}
