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
	        try {
				doWork();
			} catch (RetrievalException|PutException e) {
				e.printStackTrace();
			}
	    }
		
		abstract void doWork() throws RetrievalException, PutException;
	}

	private class ConsumerThread extends WorkerThread {
		
		public ConsumerThread(ProducerConsumer pc, int startKey, int endKey) {
			super(pc, startKey, endKey);
		}
		
		@Override
	    public void doWork() throws RetrievalException {
			pc.consumer(startKey, endKey);
	    }
	}
	
	private class ProducerThread extends WorkerThread {
		
		public ProducerThread(ProducerConsumer pc, int startKey, int endKey) {
			super(pc, startKey, endKey);
		}
		
	    public void doWork() throws PutException {
			pc.producer(startKey, endKey);
	    }
	}

	private static ProducerConsumer pc;

	@BeforeClass
	public static void setUpBeforeClass() throws ClientException, IOException {
		pc = new ProducerConsumer( Util.getTestGridConfig() );
	}
	
	@Test
	public void testProduce_PutAValue() throws ClientException, IOException {
		pc.producer(1, 1);
	}

	@Test(timeout=10_000)
	public void testProducerConsumer_threadedConsumerBeforeProducer() throws RetrievalException, InterruptedException {
		ConsumerThread ct = new ConsumerThread(pc, 3, 1000);
		ProducerThread pt = new ProducerThread(pc, 3, 1000);

		ct.start();
		pt.start();
		
		ct.join();
		pt.join();
	}
	
	@Test(timeout=10_000)
	public void testProducerConsumer_threadedProducerBeforeConsumer() throws RetrievalException, InterruptedException {
		ProducerThread pt = new ProducerThread(pc, 1_001, 10_000);
		ConsumerThread ct = new ConsumerThread(pc, 1_001, 10_000);
		
		pt.start();
		ct.start();
		
		pt.join();
		ct.join();
	}
	
	public static void main(String[] args) {
		Util.runTests(ProducerConsumerTest.class);
	}
}
