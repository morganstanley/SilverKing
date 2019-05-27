package com.ms.silverking.cloud.dht.client.apps.test;

import static com.ms.silverking.cloud.dht.client.example.ProducerConsumerTest.TIMEOUT_MILLIS;

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
		private   final int startKey;
		private   final int endKey;
		
		public WorkerThread(ProducerConsumer pc, int startKey, int endKey) {
			this.pc = pc;
			this.startKey = startKey;
			this.endKey   = endKey;
		}
		
	    public void run() {
	    	for (int i = startKey; i <= endKey; i++)
	    		doWork(i);
	    }
		
		abstract void doWork(int i);
	}

	private class ConsumerThread extends WorkerThread {
		
		public ConsumerThread(ProducerConsumer pc, int startKey, int endKey) {
			super(pc, startKey, endKey);
		}
		
		@Override
	    public void doWork(int i) {
			try {
				pc.consume(i);
			} catch (RetrievalException e) {
				throw new RuntimeException("Consumer Exception with " + i, e);
			}
	    }
	}
	
	private class ProducerThread extends WorkerThread {
		
		public ProducerThread(ProducerConsumer pc, int startKey, int endKey) {
			super(pc, startKey, endKey);
		}
		
	    public void doWork(int i) {
			try {
				pc.produce(i);
			} catch (PutException e) {
				throw new RuntimeException("Producer Exception with " + i, e);
			}
	    }
	}
	
	private static ProducerConsumer pc;

	@BeforeClass
	public static void setUpBeforeClass() throws ClientException, IOException {
		String ns = com.ms.silverking.cloud.dht.client.example.ProducerConsumer.pcNamespace+"2";	// ns needs to be unique across all tests running (ProducerConsumerTest in cloud.dht.client.example is already using pcNamespace)
		pc = new ProducerConsumer( Util.getTestGridConfig(), ns, null, (Util.isSetQuietMode())? 0 : 1 );
	}
	
	@Test
	public void testProduceAndConsume() throws ClientException, IOException {
		pc.produce(1);
		pc.consume(1);
	}
	
	// FIXME:bph: for some reason ant doesn't like this, eventually get a "Producer Exception with XXXX", sometimes it's 6072, sometimes 5300, etc..
	// this runs just fine outside of ant i.e. running main() directly
//	@Test(timeout=TIMEOUT_MILLIS)
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
