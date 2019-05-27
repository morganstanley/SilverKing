package com.ms.silverking.util.memory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import static com.ms.silverking.testing.Util.getTestMessage;

import org.junit.Before;
import org.junit.Test;

public class JVMMonitorTest {

    private JVMMonitor jm;
    
    @Before
    public void setUp() throws Exception {
        jm = new JVMMonitor(0, 0, 0, true, 0, null);
    }

    @Test
    public void testMemoryLow() {
        assertFalse(jm.memoryLow());
    }

    @Test
    public void testbytesToMB() {
        Object[][] testCases = {
            {-1024L, -.0009765625d},
            {0L,                0d},
            {1024L,   .0009765625d},
            {524_288L,         .5d},
            {1_048_576L,        1d},
        };
            
        for (Object[] testCase : testCases) {
            long bytes        =   (long)testCase[0];
            double expectedMb = (double)testCase[1];
            
            checkBytesToMb(bytes, expectedMb);
        }
    }
    
    private void checkBytesToMb(long bytes, double expectedMb) {
        assertEquals( getTestMessage("bytesToMB", bytes), expectedMb, jm.bytesToMB(bytes), 0);
    }
}
