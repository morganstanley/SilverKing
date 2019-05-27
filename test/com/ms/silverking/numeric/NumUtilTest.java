package com.ms.silverking.numeric;

import static com.ms.silverking.testing.Util.copy;
import static com.ms.silverking.testing.Util.createToString;
import static com.ms.silverking.testing.Util.double_maxVal;
import static com.ms.silverking.testing.Util.double_nan;
import static com.ms.silverking.testing.Assert.exceptionNameChecker;
import static com.ms.silverking.testing.Util.getTestMessage;
import static com.ms.silverking.testing.Util.int_maxVal;
import static com.ms.silverking.testing.Util.int_minVal;
import static com.ms.silverking.testing.Util.long_maxVal;
import static com.ms.silverking.testing.Util.long_minVal;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.google.common.primitives.Doubles;
import com.ms.silverking.testing.Util.ExceptionChecker;

public class NumUtilTest {
	
	private static final int[] int_zero          = {0};
	private static final int[] int_one           = {1};
	private static final int[] int_zeroOne       = {0,  1};
	private static final int[] int_zeroNegOne    = {0, -1};
	private static final int[] int_zeroNegOneTwo = {0, -1,  2};
	private static final int[] int_twoOneZero    = {2,  1,  0};
	private static final int[] int_oneToHundred  = {1,  2, 99, 100};

	private static final double[] double_zero         = {0};
	private static final double[] double_zeroOne      = {0, -.1};
	private static final double[] double_zeroOneTwo   = {0, -.1, .2};
	private static final double[] double_oneToHundred = {1,  2, 99, 100.05};

	private static final double[] double_one         = {1};
	private static final double[] double_oneTwo      = {1, 2};
	private static final double[] double_oneTwoThree = {1, 2, 3};
	
	// precision 5 and leading zeros after decimal don't count towards precision
	private static final double[] double_normalizedOneTwo       = {.33333, .66667};
	private static final double[] double_normalizedOneTwoThree  = {.16667, .33333, .50000};
	private static final double[] double_normalizedOneToHundred = {.0049493, .0098985, .48998, .49517};
	// 1      = .00494927 => .0049493
	// 2      = .00989854 => .0098985
	// 99     = .489977   => .48998
	// 100.05 = .495174   => .49517
	
	private static final MathContext mathContext = new MathContext(5);

	@Test
	public void testAddAndSub() {
		Object[][] testCases = {
			{int_zero,       0, int_zero,           int_zero},
			{int_one,        1, new int[]{2},       int_zero},
			{int_zeroOne,   -1, new int[]{-1, 0},   new int[]{1, 2}},
			{int_twoOneZero, 5, new int[]{7, 6, 5}, new int[]{-3, -4, -5}},
		};
		
		for (Object[] testCase : testCases) {
			int[] values      = (int[])testCase[0];
			int constant      =   (int)testCase[1];
			int[] expectedAdd = (int[])testCase[2];
			int[] expectedSub = (int[])testCase[3];
			
			checkAdd(values,  constant, expectedAdd);
			checkSub(values, -constant, expectedAdd);
			checkAdd(values, -constant, expectedSub);
			checkSub(values,  constant, expectedSub);
		}
	}
	
	private void checkAdd(int[] values, int constant, int[] expected) {
		int[] valuesCopy = copy(values);
		NumUtil.add(valuesCopy, constant);
		assertArrayEquals(getTestMessage("add", createToString(values), constant, createToString(expected), createToString(valuesCopy)), expected, valuesCopy);
	}
	
	private void checkSub(int[] values, int constant, int[] expected) {
		int[] valuesCopy = copy(values);
		NumUtil.sub(valuesCopy, constant);
		assertArrayEquals(getTestMessage("sub", createToString(values), constant, createToString(expected), createToString(valuesCopy)), expected, valuesCopy);
	}

	@Test
	public void testSum() {
		Object[][] testCases = {
			{double_zero,              0d, int_zero,             0},
			{double_zeroOne,         -.1d, int_zeroNegOne,      -1},
			{double_zeroOneTwo,       .1d, int_zeroNegOneTwo,    1},
			{double_oneToHundred, 202.05d, int_oneToHundred,   202},
		};
		
		for (Object[] testCase : testCases) {
			double[] valuesDouble = (double[])testCase[0];
			double expectedDouble =   (double)testCase[1];
			int[] valuesInt       =    (int[])testCase[2];
			int expectedInt       =      (int)testCase[3];
		
		
			checkSum_Double(valuesDouble, expectedDouble);
			checkSum_Int(valuesInt, expectedInt);
			checkSum_BigDecimal(bigDecimalAsList(valuesDouble), createBigDecimal(expectedDouble));
			checkSum_BigDecimal(bigDecimalAsList(valuesInt),    createBigDecimal(expectedInt));
		}
		
		checkSum_BigDecimal(bigDecimalAsList(double_normalizedOneToHundred), createBigDecimal(.9999978d));
	}

	private void checkSum_Double(double[] values, double expected) {
		assertEquals(getTestMessage("sum_Double", createToString(values)), expected, NumUtil.sum(Doubles.asList(values)), 0);
	}

	private void checkSum_Int(int[] values, int expected) {
		assertEquals(getTestMessage("sum_Int", createToString(values)), expected, NumUtil.sum(values));
	}

	private void checkSum_BigDecimal(List<BigDecimal> values, BigDecimal expected) {
		assertEquals(getTestMessage("sum_BigDecimal", values), expected, NumUtil.sum(values, mathContext));
	}
	
	private List<BigDecimal> bigDecimalAsList(int[] values) {
		List<BigDecimal> valuesList = new ArrayList<>();
		for (int value : values)
			valuesList.add(createBigDecimal(value));
		return valuesList;
	}
	
	private BigDecimal createBigDecimal(int value) {
		return new BigDecimal(value);
	}

	@Test
	public void testDoubleToBD() {
		Object[][] testCases = {
			{double_zero,         double_zero},
			{double_zeroOne,      double_zeroOne},
			{double_zeroOneTwo,   double_zeroOneTwo},
			{double_oneToHundred, double_oneToHundred},
		};
		
		for (Object[] testCase : testCases) {
			double[] values   = (double[])testCase[0];
			double[] expected = (double[])testCase[1];
		
			checkDoubleToBD(values, bigDecimalAsList(expected));
		}
	}
	
	private List<BigDecimal> bigDecimalAsList(double... values) {
		List<BigDecimal> valuesList = new ArrayList<>();
		for (double value : values)
			valuesList.add( createBigDecimal(value) );
		return valuesList;
	}
	
	private BigDecimal createBigDecimal(double value) {
		return new BigDecimal(value, mathContext);
//		return new BigDecimal(Double.toString(value), mathContext);
	}

	private void checkDoubleToBD(double[] values, List<BigDecimal> expected) {
		assertEquals(getTestMessage("doubleToBD", createToString(values)), expected, NumUtil.doubleToBD(Doubles.asList(values), mathContext));
	}

	@Test(expected=RuntimeException.class)
	public void testNormalizeAsBD_Exception() {
		NumUtil.normalizeAsBD(Doubles.asList(-1), mathContext);
	}

	@Test
	public void testNormalizeAsBD_Exceptions() {
		Object[][] testCases = {
			{double_zero,    IllegalArgumentException.class},
			{double_zeroOne,         RuntimeException.class},
		};
			
		for (Object[] testCase : testCases) {
			double[] values                 = (double[])testCase[0];
			Class<?> expectedExceptionClass = (Class<?>)testCase[1];

			String testMessage = getTestMessage("normalizeAsBD_Exceptions", values);
			ExceptionChecker ec = new ExceptionChecker() { @Override public void check(){ checkNormalizeAsBD(values, null); } };
			exceptionNameChecker(ec, testMessage, expectedExceptionClass);
		}
	}
	
	@Test
	public void testNormalizeAsBD() {
		Object[][] testCases = {
			{double_one,          double_one},
			{double_oneTwo,       double_normalizedOneTwo},
			{double_oneTwoThree,  double_normalizedOneTwoThree},
			{double_oneToHundred, double_normalizedOneToHundred},
		};

		for (Object[] testCase : testCases) {
			double[] values   = (double[])testCase[0];
			double[] expected = (double[])testCase[1];
		
			checkNormalizeAsBD(values, bigDecimalAsList(expected));
		}
	}
	
	private void checkNormalizeAsBD(double[] values, List<BigDecimal> expected) {
		assertEquals(getTestMessage("normalizeAsBD", createToString(values)), expected, NumUtil.normalizeAsBD(Doubles.asList(values), mathContext));
	}

	@Test
	public void testLog2_Exceptions() {
		int[] testCases = {
			0,
			int_maxVal+1,
		};
			
		for (int testCase : testCases) {
			String testMessage = getTestMessage("log2_Exceptions", testCase); 
			ExceptionChecker ecLog2 = new ExceptionChecker() { @Override public void check(){ checkLog2(testCase, -1); } };
			exceptionNameChecker(ecLog2, testMessage, ArithmeticException.class);

			testMessage = getTestMessage("log2OfPerfectPower_Exceptions", testCase);
			ecLog2 = new ExceptionChecker() { @Override public void check(){ checkLog2OfPerfectPower(testCase, -1); } };
			exceptionNameChecker(ecLog2, testMessage, ArithmeticException.class);
		}
	}
	
	@Test
	public void testLog2AndIsPowerOf2() {
		Object[][] testCases = {
			{         1,  0, 0},
			{         2,  1, 1},
			{         3,  1, 0},
			{         4,  2, 2},
			{       256,  8, 8},
			{int_maxVal, 30, 0},
		};
		
		for (Object[] testCase : testCases) {
			int value                   = (int)testCase[0];
			int expectedLog             = (int)testCase[1];
			int expectedLogPerfectPower = (int)testCase[2];
			boolean expectedIsPowerOf2  = (value == 1 || expectedLogPerfectPower > 0);

			checkLog2(              value, expectedLog);
			checkLog2OfPerfectPower(value, expectedLogPerfectPower);
			checkIsPowerOf2(        value, expectedIsPowerOf2);
		}
	}
	
	private void checkLog2(int value, int expected) {
		assertEquals(getTestMessage("log2", value), expected, NumUtil.log2(value));
	}
	
	private void checkLog2OfPerfectPower(int value, int expected) {
		assertEquals(getTestMessage("log2OfPerfectPower", value), expected, NumUtil.log2OfPerfectPower(value));
	}
	
	private void checkIsPowerOf2(int value, boolean expected) {
		assertEquals(getTestMessage("isPowerOf2", value), expected, NumUtil.isPowerOf2(value));
	}
	
	@Test
	public void testLog() {
		Object[][] testCases = {
			{  -1d,      -1d, double_nan, 0},
			{   0d,       0d, double_nan, 0},
			{   1d,       1d, double_nan, 0},
			{  .5d,      .5d,         1d, 0},
			{   3d,       9d,         2d, 2},
			{3.33d, 11.0889d,         2d, 2},
			{ 7.1d,   50.41d,         2d, 2},
			
			{ 1.5d, 57.6650390628d, 10.00000000001283d, int_maxVal},
			{double_maxVal, double_maxVal, 1d, 1},
		};
		
		for (Object[] testCase : testCases) {
			double base               = (double)testCase[0];
			double value              = (double)testCase[1];
			double expectedLog_Double = (double)testCase[2];
			int expectedLog_Int       =    (int)testCase[3];

			checkLog_Double(base, value, expectedLog_Double);
			checkLog_Int((int)base, (int)value, expectedLog_Int);
		}
	}
	
	private void checkLog_Double(double base, double value, double expected) {
		assertEquals(getTestMessage("log_Double", base, value), expected, NumUtil.log(base, value), 0);
	}

	private void checkLog_Int(int base, int value, int expected) {
		assertEquals(getTestMessage("log_Int", base, value), expected, NumUtil.log(base, value));
	}
	
	@Test
	public void testPow() {
		Object[][] testCases = {
			{ 1L, 1L,                1L,   1},
			{10L, 2L,              100L, 100},
			{ 2L, 37L, 137_438_953_472L,   0},
			
			{(long)int_maxVal+1, 1L, (long)int_maxVal+1, int_minVal},
		};
		
		for (Object[] testCase : testCases) {
			long base             = (long)testCase[0];
			long exponent         = (long)testCase[1];
			long expectedPow_Long = (long)testCase[2];
			int expectedPow_Int   =  (int)testCase[3];

			checkPow_Long(base, exponent, expectedPow_Long);
			checkPow_Int((int)base, (int)exponent, expectedPow_Int);
		}
	}

	private void checkPow_Long(long base, long exponent, long expected) {
		assertEquals(getTestMessage("pow_Long", base, exponent), expected, NumUtil.pow(base, exponent));
	}

	private void checkPow_Int(int base, int exponent, int expected) {
		assertEquals(getTestMessage("pow_Int", base, exponent), expected, NumUtil.pow(base, exponent));
	}
	
	@Test
	public void testLongHashCode() {
		Object[][] testCases = {
			{0xFFFFFFFFFFFFFFFFL, 0x00000000},
			{0xFFFFFFFF00000000L, 0xFFFFFFFF},
			{0x00000000FFFFFFFFL, 0xFFFFFFFF},
			{0x0000000000000000L, 0x00000000},
		};
		
		for (Object[] testCase : testCases) {
			long value   = (long)testCase[0];
			int expected =  (int)testCase[1];

			checkLongHashCode(value, expected);
		}
	}

	private void checkLongHashCode(long value, int expected) {
		String param = "0x"+Long.toHexString(value);
		assertEquals(getTestMessage("longHashCode", param.toUpperCase()), expected, NumUtil.longHashCode(value));
	}
	
	@Test
	public void testAddWithClamp() {
		long[][] testCases = {
			{          0,           0,             0},
			{long_minVal, long_maxVal,            -1},
			{long_maxVal, long_minVal,            -1},
			{long_minVal, long_minVal,             0},
			{long_minVal, long_minVal+1, long_minVal},
			{long_maxVal, long_maxVal,   long_maxVal},
		};
		
		for (long[] testCase : testCases) {
			long a        = testCase[0];
			long b        = testCase[1];
			long expected = testCase[2];

			checkAddWithClamp(a, b, expected);
		}
	}

	private void checkAddWithClamp(long a, long b, long expected) {
		assertEquals(getTestMessage("addWithClamp", a, b), expected, NumUtil.addWithClamp(a, b));
	}
	
}
