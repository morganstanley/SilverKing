package com.ms.silverking.cloud.dht.client;

import static com.ms.silverking.cloud.dht.client.TestUtil.absMillisProvider;
import static com.ms.silverking.cloud.dht.client.TestUtil.absNanosProvider;
import static com.ms.silverking.cloud.dht.client.TestUtil.constantProvider;
import static com.ms.silverking.cloud.dht.client.TestUtil.relNanosProvider;
import static com.ms.silverking.cloud.dht.client.TestUtil.test_GetVersion;
import static com.ms.silverking.cloud.dht.client.TestUtil.v;
import static com.ms.silverking.testing.AssertFunction.checkHashCodeEquals;
import static com.ms.silverking.testing.AssertFunction.checkHashCodeNotEquals;
import static com.ms.silverking.testing.AssertFunction.test_NotEquals;
import static com.ms.silverking.testing.AssertFunction.test_FirstEqualsSecond_FirstNotEqualsThird;

import org.junit.Test;

public class ConstantVersionProviderTest {

	private static final long vCopy = 0;
	private static final long vDiff = 1;
	
	private static final ConstantVersionProvider constantProviderCopy = new ConstantVersionProvider(vCopy);
	private static final ConstantVersionProvider constantProviderDiff = new ConstantVersionProvider(vDiff);

	@Test
	public void testVersion() {
		Object[][] testCases = {
			{v,     constantProvider},
			{vCopy, constantProviderCopy},
			{vDiff, constantProviderDiff},
		};
		
		test_GetVersion(testCases);
	}
	
	@Test
	public void testHashCode() {
		checkHashCodeEquals(   constantProvider, constantProvider);
		checkHashCodeEquals(   constantProvider, constantProviderCopy);
		checkHashCodeNotEquals(constantProvider, constantProviderDiff);
	}
	
	@Test
	public void testEqualsObject() {
		ConstantVersionProvider[][] testCases = {
			{constantProvider,     constantProvider,     constantProviderDiff},
			{constantProviderCopy, constantProvider,     constantProviderDiff},
			{constantProviderDiff, constantProviderDiff, constantProvider},
		};
		test_FirstEqualsSecond_FirstNotEqualsThird(testCases);
		
		test_NotEquals(new Object[][]{
			{constantProvider, absMillisProvider},
			{constantProvider, absNanosProvider},
			{constantProvider, relNanosProvider},
		});
	}
}
