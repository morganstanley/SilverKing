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
import com.ms.silverking.time.RelNanosTimeSource;
import com.ms.silverking.time.TimerDrivenTimeSource;

public class RelNanosVersionProviderTest {

    private static final RelNanosTimeSource rntsCopy = new TimerDrivenTimeSource(1);
    private static final RelNanosTimeSource rntsDiff = SystemTimeUtil.skSystemTimeSource;
    
    private static final RelNanosVersionProvider relNanosProviderCopy = new RelNanosVersionProvider(rntsCopy);
    private static final RelNanosVersionProvider relNanosProviderDiff = new RelNanosVersionProvider(rntsDiff);

    // FUTURE:bph: comments
    
    @Test
    public void testVersion() {
        Object[][] testCases = {
//            {0L, relNanosProvider},    // TimerDrivenTimeSource - relTimeNanos is dynamic
//            {0L, relNanosProviderCopy},    // TimerDrivenTimeSource - relTimeNanos is dynamic
//            {0L, relNanosProviderDiff},    // SystemTimeSource - relTimeNanos is dynamic
        };
        
        test_GetVersion(testCases);
    }
    
    @Test
    public void testHashCode() {
        checkHashCodeEquals(   relNanosProvider, relNanosProvider);
        checkHashCodeNotEquals(relNanosProvider, relNanosProviderCopy);    // TimerDrivenTimeSource - SafeTimer - uses object identity for hashCode
        checkHashCodeNotEquals(relNanosProvider, relNanosProviderDiff);
    }
    
    public void testEqualsObject() {
        RelNanosVersionProvider[][] testCases = {
            {relNanosProvider,     relNanosProvider,     relNanosProviderDiff},
            {relNanosProviderDiff, relNanosProviderDiff, relNanosProvider},
        };
        test_FirstEqualsSecond_FirstNotEqualsThird(testCases);
        
        test_NotEquals(new Object[][]{
            {relNanosProvider, absMillisProvider},
            {relNanosProvider, absNanosProvider},
            {relNanosProvider, constantProvider},
            {relNanosProvider, relNanosProviderCopy},    // TimerDrivenTimeSource - SafeTimer - uses object identity for equals
        });
    }
}
