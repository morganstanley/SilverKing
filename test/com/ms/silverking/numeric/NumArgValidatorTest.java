package com.ms.silverking.numeric;

import static com.ms.silverking.testing.Util.double_nan;
import static com.ms.silverking.testing.Util.double_negInf;
import static com.ms.silverking.testing.Util.double_posInf;
import static com.ms.silverking.testing.Assert.exceptionNameChecker;
import static com.ms.silverking.testing.Util.getTestMessage;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.ms.silverking.testing.Util.ExceptionChecker;

public class NumArgValidatorTest {

	private static final double double_minVal  = Double.MIN_VALUE;
	private static final double double_maxVal  = Double.MAX_VALUE;
	private static final double double_minNorm = Double.MIN_NORMAL;
	
	@Test
	public void testEnsureRegularDouble_Exceptions() {
		double[] testCases = {
			double_nan,
			double_negInf,
			double_posInf,
		};
		
		for (double testCase : testCases) {
			String testMessage = getTestMessage("ensureRegularDouble_Exceptions", testCase); 
			ExceptionChecker ec = new ExceptionChecker() { @Override public void check(){ checkEnsureRegularDouble(testCase); } };
			exceptionNameChecker(ec, testMessage, IllegalArgumentException.class);
		}
	}
	
	@Test
	public void testEnsureRegularDouble() {
		double[] testCases = {
			-double_maxVal,
			0.0d,
			 double_minVal,
			 double_minNorm,
		 	 double_maxVal,
		};
		
		for (double testCase : testCases) 
			checkEnsureRegularDouble(testCase);
	}
	
	private void checkEnsureRegularDouble(double d) {
		NumArgValidator.ensureRegularDouble(d);
		assertTrue(true);
	}
	
	@Test
	public void testEnsurePositiveRegularDouble_Exceptions() {
		double[] testCases = {
			 double_nan,
			 double_negInf,
			 double_posInf,
			-double_maxVal,
			0.0d,
		};
		
		for (double testCase : testCases) {
			String testMessage = getTestMessage("ensurePositiveRegularDouble_Exceptions", testCase); 
			ExceptionChecker ec = new ExceptionChecker() { @Override public void check(){ checkEnsurePositiveRegularDouble(testCase); } };
			exceptionNameChecker(ec, testMessage, IllegalArgumentException.class);
		}
	}
	
	@Test
	public void testEnsurePositiveRegularDouble() {
		double[] testCases = {
			double_minVal,
			double_minNorm,
			double_maxVal,
		};
		
		for (double testCase : testCases) 
			checkEnsurePositiveRegularDouble(testCase);
	}
	
	private void checkEnsurePositiveRegularDouble(double d) {
		NumArgValidator.ensurePositiveRegularDouble(d);
		assertTrue(true);
	}

}
