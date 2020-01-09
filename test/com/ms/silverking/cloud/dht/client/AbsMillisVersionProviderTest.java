package com.ms.silverking.cloud.dht.client;

import static com.ms.silverking.cloud.dht.client.TestUtil.absMillisProvider;
import static com.ms.silverking.cloud.dht.client.TestUtil.absNanosProvider;
import static com.ms.silverking.cloud.dht.client.TestUtil.constantProvider;
import static com.ms.silverking.cloud.dht.client.TestUtil.relNanosProvider;
import static com.ms.silverking.cloud.dht.client.TestUtil.test_GetVersion;
import static com.ms.silverking.testing.AssertFunction.checkHashCodeEquals;
import static com.ms.silverking.testing.AssertFunction.checkHashCodeNotEquals;
import static com.ms.silverking.testing.AssertFunction.test_FirstEqualsSecond_FirstNotEqualsThird;
import static com.ms.silverking.testing.AssertFunction.test_NotEquals;

import org.junit.Test;

import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.time.AbsMillisTimeSource;
import com.ms.silverking.time.ConstantAbsMillisTimeSource;

public class AbsMillisVersionProviderTest {

    private static final AbsMillisTimeSource amtsCopy = new ConstantAbsMillisTimeSource(0);
    private static final AbsMillisTimeSource amtsDiff = SystemTimeUtil.skSystemTimeSource;
    
    private static final AbsMillisVersionProvider absMillisProviderCopy = new AbsMillisVersionProvider(amtsCopy);
    private static final AbsMillisVersionProvider absMillisProviderDiff = new AbsMillisVersionProvider(amtsDiff);

    @Test
    public void testVersion() {
        Object[][] testCases = {
            {0L, absMillisProvider},
            {0L, absMillisProviderCopy},
//            {, absMillisProviderDiff},    // FUTURE:bph: SystemTimeSource - absTimeMillis is dynamic
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
        AbsMillisVersionProvider[][] testCases = {
            {absMillisProvider,     absMillisProvider,     absMillisProviderDiff},
            {absMillisProviderCopy, absMillisProvider,     absMillisProviderDiff},
            {absMillisProviderDiff, absMillisProviderDiff, absMillisProvider},
        };
        test_FirstEqualsSecond_FirstNotEqualsThird(testCases);
        
        test_NotEquals(new Object[][]{
            {absMillisProvider, absNanosProvider},
            {absMillisProvider, constantProvider},
            {absMillisProvider, relNanosProvider},
        });
    }
}
