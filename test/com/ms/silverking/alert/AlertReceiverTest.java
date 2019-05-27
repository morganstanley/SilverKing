package com.ms.silverking.alert;

import static com.ms.silverking.alert.AlertTest.alertTestExpectedToString;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class AlertReceiverTest {

    private TestAlertReceiver receiver;
    
    public AlertReceiverTest() {
        receiver = new TestAlertReceiver();
    }
    
    @Test
    public void testSendAlert() {
        assertEquals("", receiver.getAlert());
        receiver.sendAlert(AlertTest.alert);
        assertEquals(alertTestExpectedToString, receiver.getAlert());
    }

}
