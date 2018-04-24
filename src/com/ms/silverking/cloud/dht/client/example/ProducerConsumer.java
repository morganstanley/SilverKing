package com.ms.silverking.cloud.dht.client.example;

import java.io.IOException;

import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.PutException;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;

public class ProducerConsumer {        
    SynchronousNamespacePerspective<Integer, Integer> syncNSP;
    
    public static final String pcNamespace = "_ProducerConsumer";
    
    private enum Mode {Producer, Consumer};

    public ProducerConsumer(SKGridConfiguration gridConfig) throws ClientException, IOException {
    	this(gridConfig, "");
    }
    
    ProducerConsumer(SKGridConfiguration gridConfig, String nsSuffix) throws ClientException, IOException {
        syncNSP = new DHTClient().openSession(gridConfig)
                .openSyncNamespacePerspective(pcNamespace+nsSuffix, Integer.class, Integer.class);
    }
    
    public void run(Mode mode, int startKey, int endKey) throws ClientException {
        switch (mode) {
        case Producer: producer(startKey, endKey); break;
        case Consumer: consumer(startKey, endKey); break;
        default: throw new RuntimeException("Panic");
        }
    }
    
    public void producer(int startKey, int endKey) throws PutException {
        for (int i = startKey; i <= endKey; i++) {
            System.out.printf("Writing %d\t", i);
            syncNSP.put(i, i);
            System.out.printf("Wrote %d\n", i);
        }
    }
    
    public void consumer(int startKey, int endKey) throws RetrievalException {
        for (int i = startKey; i <= endKey; i++) {
            int val;
            System.out.printf("Waiting for %d\t", i);
            val = syncNSP.waitFor(i);
            System.out.printf("Received %d\n", val);
        }
    }
    
    private static void usage() {
        System.out.println("args: <gridConfig> <mode [Producer|Consumer]> <startKey> <endKey>");
    }
    
    public static void main(String[] args) {
        try {
            if (args.length != 4) {
                usage();
            } else {
                ProducerConsumer   pc;
                SKGridConfiguration gridConfig;
                Mode               mode;
                int                startKey;
                int                endKey;
                
                gridConfig = SKGridConfiguration.parseFile(args[0]);
                mode = Mode.valueOf(args[1]);
                startKey = Integer.parseInt(args[2]);
                endKey = Integer.parseInt(args[3]);
                pc = new ProducerConsumer(gridConfig);
                pc.run(mode, startKey, endKey);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
