package com.ms.silverking.cloud.dht.client.apps.test;

import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import com.ms.silverking.cloud.dht.ConsistencyProtocol;
import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.StorageType;
import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.Namespace;
import com.ms.silverking.cloud.dht.client.PutException;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;

public class WriteReadTest implements Runnable {
    private final PrintStream out;
    private final PrintStream err;
    private final DHTSession  session;
    private final Namespace   ns;
    private final AtomicInteger	threadIndex;
    private final int numThreads;
    private final int keysPerThread;
    private final int updatesPerKey;
    private final Semaphore	runningSem;

    private static final String nsBase = "WriteReadTest.";
    
    private static final boolean	verbose = false;
    
    public WriteReadTest(SKGridConfiguration gridConfig, PrintStream out, PrintStream err,
    		int numThreads, int keysPerThread, int updatesPerKey) throws ClientException, IOException {
        String      nsName;
        NamespaceOptions    nsOptions;
        
        nsName = nsBase + System.currentTimeMillis() +".";
        this.out = out;
        this.err = err;
        this.numThreads = numThreads;
        this.keysPerThread = keysPerThread;
        this.updatesPerKey = updatesPerKey;
        session = new DHTClient().openSession(gridConfig);
        nsOptions = session.getDefaultNamespaceOptions()
        				.versionMode(NamespaceVersionMode.SYSTEM_TIME_NANOS)
                        .storageType(StorageType.FILE)
                        .consistencyProtocol(ConsistencyProtocol.TWO_PHASE_COMMIT)
                        .defaultPutOptions(session.getDefaultPutOptions()
                                            .compression(Compression.NONE)
                                            .checksumType(ChecksumType.NONE));
        ns = session.createNamespace(nsName, nsOptions);
        threadIndex = new AtomicInteger();
        runningSem = new Semaphore(-numThreads + 1);
    }
    
	public void doTest() {
        Stopwatch   runSW;
        
        runSW = new SimpleStopwatch();
        
        for (int i = 0; i < numThreads; i++) {
        	new Thread(this).start();
        }
        try {
			runningSem.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
        runSW.stop();
        out.printf("Elapsed: %f\n", runSW.getElapsedSeconds());
	}
	
	public void run() {
    	try {
    		singleThreadTest(threadIndex.getAndIncrement());
    	} finally {
    		runningSem.release();
    	}
	}
    
    private void singleThreadTest(int index) {
//    	out.printf("Thread: %d\n", index);
        SynchronousNamespacePerspective<String,Integer>	nsp;
        
        nsp = ns.openSyncPerspective(String.class, Integer.class);
    	for (int i = 0; i < keysPerThread; i++) {
//    		System.out.println("thread: " + index + " key: " + i);
    		for (int j = 0; j < updatesPerKey; j++) {
    			String	key;
    			
    			key = index +"."+ i;
    			try {
        			int		value;
        			
        			if (verbose) {
        				out.printf("%s\n", key);
        			}
					nsp.put(key, j);
	    			value = nsp.get(key);
	    			if (value != j) {
	    				out.printf("Read mismatch:\t%s\t%d\t%d\n", key, value, j);
	    			}
				} catch (PutException e) {
					e.printStackTrace();
    				out.printf("PutException:\t%s\t%s\n", key, e);
				} catch (RetrievalException e) {
					e.printStackTrace();
    				out.printf("RetrievalException:\t%s\t%s\n", key, e);
				}
    		}
    	}
	}

	/**
     * @param args
     */
    public static void main(String[] args) {
        try {
            if (args.length != 4) {
                System.err.println("Usage: <gridConfig> <numThreads> <keysPerThread> <updatesPerKey>");
            } else {
                WriteReadTest	wrt;
                String          gridConfig;
                int             numThreads;
                int				keysPerThread;
                int				updatesPerKey;
             
                gridConfig = args[0];
                numThreads = Integer.parseInt(args[1]);
                keysPerThread = Integer.parseInt(args[2]);
                updatesPerKey = Integer.parseInt(args[3]);
                wrt = new WriteReadTest(SKGridConfiguration.parseFile(gridConfig),
                                         System.out, System.err,
                                         numThreads, keysPerThread, updatesPerKey);
                wrt.doTest();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
