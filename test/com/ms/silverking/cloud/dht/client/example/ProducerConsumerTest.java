package com.ms.silverking.cloud.dht.client.example;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.client.PutException;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.testing.Util;

import com.ms.silverking.testing.annotations.SkLarge;

@SkLarge
public class ProducerConsumerTest {

	public static final int TIMEOUT_MILLIS = 10_000;
	
	private abstract class WorkerThread extends Thread {
		
		protected final ProducerConsumer pc;
		protected final int startKey;
		protected final int endKey;
		
		public WorkerThread(ProducerConsumer pc, int startKey, int endKey) {
			this.pc = pc;
			this.startKey = startKey;
			this.endKey   = endKey;
		}
		
	    public void run() {
			doWork();
	    }
		
		abstract void doWork();
	}

	private class ConsumerThread extends WorkerThread {
		
		public ConsumerThread(ProducerConsumer pc, int startKey, int endKey) {
			super(pc, startKey, endKey);
		}
		
		@Override
	    public void doWork() {
			try {
				pc.consumer(startKey, endKey);
			} catch (RetrievalException e) {
				throw new RuntimeException(e);
			}
	    }
	}
	
	private class ProducerThread extends WorkerThread {
		
		public ProducerThread(ProducerConsumer pc, int startKey, int endKey) {
			super(pc, startKey, endKey);
		}
		
	    public void doWork() {
			try {
				pc.producer(startKey, endKey);
			} catch (PutException e) {
				throw new RuntimeException(e);
			}
	    }
	}

	private static ProducerConsumer pc;

	@BeforeClass
	public static void setUpBeforeClass() throws ClientException, IOException {
		pc = new ProducerConsumer( Util.getTestGridConfig() );
	}
	
	@Test
	public void testProducerAndConsumer() throws PutException, RetrievalException {
		int startKey = 1;
		int   endKey = startKey;
		pc.producer(startKey, endKey);
		pc.consumer(startKey, endKey);
	}
	
	@Test(timeout=TIMEOUT_MILLIS)
	public void testProducerConsumer_threadedProducerBeforeConsumer() throws InterruptedException {
		ProducerThread pt = new ProducerThread(pc, 2, 9_000);
		ConsumerThread ct = new ConsumerThread(pc, 2, 9_000);
		
		pt.start();
		ct.start();
		
		pt.join();
		ct.join();
	}

	@Test(timeout=TIMEOUT_MILLIS)
	public void testProducerConsumer_threadedConsumerBeforeProducer() throws InterruptedException {
		ConsumerThread ct = new ConsumerThread(pc, 9_001, 10_000);
		ProducerThread pt = new ProducerThread(pc, 9_001, 10_000);

		ct.start();
		pt.start();
		
		ct.join();
		pt.join();
	}
	
	public static void main(String[] args) {
		Util.runTests(ProducerConsumerTest.class);
	}
}
