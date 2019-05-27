package com.ms.silverking.numeric;

import static com.ms.silverking.testing.Util.createToString;
import static com.ms.silverking.testing.Util.getTestMessage;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class EntropyCalculatorTest {

	@Test
	public void testComputeEntropy() {
		Object[][] testCases = {
			{new byte[]{0},                                      0.0d},
			{new byte[]{0, 0},                                   0.0d},
			{new byte[]{0, 1},                                   1.0d},
			{new byte[]{1, 0},                                   1.0d},
			{new byte[]{0, 1, 2},                  1.584962500721156d},
			{new byte[]{2, 1, 0},                  1.584962500721156d},
			{new byte[]{-1, 0, 1},                 1.584962500721156d},
			{new byte[]{-3, -2, -1, 0, 1, 2, 3},   2.8073549220576046},
			{new byte[]{-5, -5, -5, 5, 5, 5},                    1.0d},
			{new byte[]{-5,  5, -5, 5, -5, 5},                   1.0d},
			{new byte[]{127, -128, 0, -128, 127}, 1.5219280948873621d},
		};
		
		for (Object[] testCase : testCases) {
			byte[] input    = (byte[])testCase[0];
			double expected = (double)testCase[1];
			
			checkComputeEntropy(input, expected);
		}
	}
	
	private void checkComputeEntropy(byte[] input, double expected) {
		assertEquals( getTestMessage("computeEntropy", "input = " + createToString(input)), expected, EntropyCalculator.computeEntropy(input), 0.0);
	}
}
