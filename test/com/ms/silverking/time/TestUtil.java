package com.ms.silverking.time;

import static org.junit.Assert.assertEquals;
import static com.ms.silverking.testing.Util.*;

public class TestUtil {

	public static final long absMillisTime  = 0;
	public static final long absNanosTime   = 0;
	
	public static final ConstantAbsMillisTimeSource constantSource =  new ConstantAbsMillisTimeSource(absMillisTime);
	public static final SystemTimeSource systemSource              =  SystemTimeSource.createWithMillisOrigin(absNanosTime);
	public static final TimerDrivenTimeSource timerSource          =  new TimerDrivenTimeSource();
	
	public static void test_AbsTimeMillis(Object[][] testCases) {
		for (Object[] testCase : testCases) {
			long expected              =                (long)testCase[0];
			AbsMillisTimeSource source = (AbsMillisTimeSource)testCase[1];
			
			assertEquals( getTestMessage("absTimeMillis", expected, source), expected, source.absTimeMillis());
		}
	}
	
	public static void test_RelMillisRemaining(Object[][] testCases) {
		for (Object[] testCase : testCases) {
			long expected              =                (long)testCase[0];
			AbsMillisTimeSource source = (AbsMillisTimeSource)testCase[1];
			long deadline              =                (long)testCase[2];
			
			assertEquals( getTestMessage("relMillisRemaining", expected, source, deadline), expected, source.relMillisRemaining(deadline));
		}
	}

	public static void test_AbsTimeNanos(Object[][] testCases) {
		for (Object[] testCase : testCases) {
			long expected             =               (long)testCase[0];
			AbsNanosTimeSource source = (AbsNanosTimeSource)testCase[1];
			
			assertEquals( getTestMessage("absTimeNanos", expected, source), expected, source.absTimeNanos());
		}
	}

	public static void test_GetNanosOriginTime(Object[][] testCases) {
		for (Object[] testCase : testCases) {
			long expected             =               (long)testCase[0];
			AbsNanosTimeSource source = (AbsNanosTimeSource)testCase[1];
			
			assertEquals( getTestMessage("getNanosOriginTime", expected, source), expected, source.getNanosOriginTime());
		}
	}
}
