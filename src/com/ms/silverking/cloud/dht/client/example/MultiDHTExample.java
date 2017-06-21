package com.ms.silverking.cloud.dht.client.example;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.ms.silverking.cloud.dht.SessionOptions;
import com.ms.silverking.cloud.dht.client.AsynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.KeyedOperationException;
import com.ms.silverking.cloud.dht.client.PutException;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;


/**
 * Example code to show connecting to multiple DHTs. Also demonstrates connecting 
 * to the same DHT with multiple sessions.
 * 
 * The code is not meant to demonstrate the proper way to use the DHT so much as
 * to demonstrate that multiple connections are possible.
 */
public class MultiDHTExample {
    private final String        namespace;
    private final AtomicInteger    threadCount;
    private SKGridConfiguration[] gridConfigurations;
    private String[] preferredServers;
    
    public MultiDHTExample(SKGridConfiguration[] gridConfigurations, String[] preferredServers) {
        namespace = new Long(System.currentTimeMillis()).toString();
        System.out.println("namespace: "+ namespace);
        threadCount = new AtomicInteger();
        
        this.gridConfigurations = gridConfigurations;
        this.preferredServers   = preferredServers;
    }
    
    public void start(int iterations) throws ClientException, IOException {
        for (int i = 0; i < gridConfigurations.length; i++) {
            new Writer(gridConfigurations[i], preferredServers[i], iterations, gridConfigurations[i].getName());
            new Reader(gridConfigurations[i], preferredServers[i], iterations, gridConfigurations[i].getName());
        }
    }
    
    abstract class Worker implements Runnable {
        protected final AsynchronousNamespacePerspective<String, String>    asyncNSP;
        private final int        iterations;
        protected final String    workID;
        private final int        sleepMillis;
        
        protected static final int    keysPerIteration = 4;
        
        Worker(SKGridConfiguration gridConfig, String preferredServer, int iterations, String workID) 
                                                    throws ClientException, IOException {
        	DHTSession	session;
        	
            this.iterations = iterations;
            this.workID = workID;
            session = new DHTClient().openSession(new SessionOptions(gridConfig, preferredServer));
            session.createNamespace(namespace);
            asyncNSP = session.openAsyncNamespacePerspective(namespace, String.class, String.class);
            sleepMillis = threadCount.getAndIncrement() * 1000;
            new Thread(this, workID).start();
        }
                
        public void run() {
            System.out.println("Running "+ Thread.currentThread().getName());
            try {
                for (int i = 0; i < iterations; i++) {
                    doWork(i);
                    try {
                        Thread.sleep(sleepMillis);
                    } catch (InterruptedException ie) {
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        
        protected abstract void doWork(int iteration) throws KeyedOperationException;
        
        protected Set<String> createKeys(int iteration, int numKeys) {
            Set<String>    keys;
            
            keys = new HashSet<String>();
            for (int i = 0; i < numKeys; i++) {
                keys.add("key."+ workID +"."+ iteration +"."+ i);
            }
            return keys;
        }        
    }
    
    class Reader extends Worker {
        Reader(SKGridConfiguration gridConfig, String preferredServer, int iterations, String workID) 
                                                            throws ClientException, IOException {
            super(gridConfig, preferredServer, iterations, workID);
        }
        
        protected void doWork(int iteration) throws RetrievalException {
            asyncNSP.get(createKeys(iteration, keysPerIteration)).waitForCompletion();
            System.out.println("Read: "+ workID +" "+ iteration);
        }
    }
    
    class Writer extends Worker {
        Writer(SKGridConfiguration gridConfig, String preferredServer, int iterations, String workID) 
                                                            throws ClientException, IOException {
            super(gridConfig, preferredServer, iterations, workID);
        }

        private Map<String,String> createKeyValues(int iteration, int numKeyValues) {
            Map<String,String>    keyValueMap;
            
            keyValueMap = new HashMap<String,String>();
            for (String key : createKeys(iteration, numKeyValues)) {
                keyValueMap.put(key, key.replace("key", "value"));
            }
            return keyValueMap;
        }
        
        protected void doWork(int iteration) throws PutException {
            asyncNSP.put(createKeyValues(iteration, keysPerIteration)).waitForCompletion();
            System.out.println("Written: "+ workID +" "+ iteration);
        }
    }
    
    public static void main(String[] args) {
        try {
            int    iterations;
            SKGridConfiguration[]    gridConfigurations;
            String[]            preferredServers;
            
            if (args.length < 2) {
                System.err.println("args: <iterations> <GridConfiguration:PreferredServer>...");
                return;
            }            
            iterations = Integer.parseInt(args[0]);
            gridConfigurations = new SKGridConfiguration[args.length - 1];
            preferredServers = new String[gridConfigurations.length];
            for (int i = 0; i < gridConfigurations.length; i++) {
                String[]    tokens;
                
                tokens = args[i + 1].split(":");
                if (tokens.length < 1 || tokens.length > 2) {
                    System.out.println("Bad GridConfiguration:PreferredServer def");
                    return;
                }                 
                gridConfigurations[i] = SKGridConfiguration.parseFile(tokens[0]);
                if (tokens.length == 2) {
                    preferredServers[i] = tokens[1];
                } else {
                    preferredServers[i] = "localhost";
                }
            }
            MultiDHTExample mDht = new MultiDHTExample(gridConfigurations, preferredServers);
            mDht.start(iterations);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
