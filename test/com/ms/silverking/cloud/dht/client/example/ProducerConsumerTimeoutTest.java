package com.ms.silverking.cloud.dht.client.example;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.runners.statements.FailOnTimeout;
import org.junit.rules.Timeout;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.time.TimeUtils;

import com.ms.silverking.testing.Util;

import com.ms.silverking.testing.annotations.SkLarge;

// from: http://stackoverflow.com/questions/37355035/junit-test-for-an-expected-timeout
//     The only one limitation: you have to place your test in separate class, because Rule applies to all tests inside it
@SkLarge
public class ProducerConsumerTimeoutTest {

    private static final int TIMEOUT_MILLIS = 15_000;

    @Rule
    public Timeout timeout = new Timeout(TIMEOUT_MILLIS) {
        public Statement apply(Statement base, Description description) {
            return new FailOnTimeout(base, TIMEOUT_MILLIS) {
                @Override
                public void evaluate() throws Throwable {
                    try {
                        super.evaluate();
                        throw new TimeoutException();
                    } catch (Exception e) {}
                }
            };
        }
    };

    @Test(expected = TimeoutException.class)
	public void testConsumer_WaitForValueNeverWritten() throws ClientException, IOException {
    	String nsSuffix = "Timeout";
		new ProducerConsumer(Util.getTestGridConfig(), nsSuffix).consumer(2, 2);
	}
	
	public static void main(String[] args) {
		Util.runTests(ProducerConsumerTimeoutTest.class);
	}

}
