package com.ms.silverking.cloud.dht.client;

import static com.ms.silverking.cloud.dht.client.TestUtil.*;
import static com.ms.silverking.testing.AssertFunction.checkHashCodeEquals;
import static com.ms.silverking.testing.AssertFunction.checkHashCodeNotEquals;
import static com.ms.silverking.testing.AssertFunction.test_NotEquals;
import static com.ms.silverking.testing.AssertFunction.test_FirstEqualsSecond_SecondNotEqualsThird;

import org.junit.Test;

import com.ms.silverking.time.AbsMillisTimeSource;
import com.ms.silverking.time.ConstantAbsMillisTimeSource;
import com.ms.silverking.time.SystemTimeSource;

public class AbsMillisVersionProviderTest {

	private static final AbsMillisTimeSource amtsCopy = new ConstantAbsMillisTimeSource(0);
	private static final AbsMillisTimeSource amtsDiff = SystemTimeSource.instance;
	
	private static final AbsMillisVersionProvider absMillisProviderCopy = new AbsMillisVersionProvider(amtsCopy);
	private static final AbsMillisVersionProvider absMillisProviderDiff = new AbsMillisVersionProvider(amtsDiff);

	@Test
	public void testVersion() {
		Object[][] testCases = {
			{0L, absMillisProvider},
			{0L, absMillisProviderCopy},
//			{, absMillisProviderDiff},	// SystemTimeSource - absTimeMillis is dynamic
		};
		
		test_GetVersion(testCases);
	}
	
	@Test
	public void testHashCode() {
		checkHashCodeEquals(   absMillisProvider, absMillisProvider);
		checkHashCodeEquals(   absMillisProvider, absMillisProviderCopy);
		checkHashCodeNotEquals(absMillisProvider, absMillisProviderDiff);
	}
	
	@Test
	public void testEqualsObject() {
		Object[][] testCases = {
			{absMillisProvider,     absMillisProvider,     absMillisProviderDiff},
			{absMillisProviderDiff, absMillisProviderDiff, absMillisProvider},
			{absMillisProviderCopy, absMillisProvider,     absMillisProviderDiff},
		};
		
		test_FirstEqualsSecond_SecondNotEqualsThird(testCases);
		test_NotEquals(new Object[][]{
			{absMillisProvider, absNanosProvider},
			{absMillisProvider, constantProvider},
			{absMillisProvider, relNanosProvider},
		});
	}
}
