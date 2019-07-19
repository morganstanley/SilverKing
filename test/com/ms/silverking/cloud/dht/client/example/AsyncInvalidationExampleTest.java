package com.ms.silverking.cloud.dht.client.example;

import java.io.IOException;

import org.junit.Test;

import com.ms.silverking.testing.Util;
import com.ms.silverking.testing.annotations.SkLarge;

@SkLarge
public class AsyncInvalidationExampleTest {

    @Test
    public void testInvalidation() throws IOException {
        TestUtil.checkValueIs(null, AsyncInvalidationExample.runExample( Util.getTestGridConfig() ));
    }
    
    public static void main(String[] args) {
        Util.runTests(AsyncInvalidationExampleTest.class);
    }

}
