package com.ms.silverking.cloud.dht.client.example;

import java.io.IOException;

import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;

public class HelloDHT {  
	
	public static String runExample(SKGridConfiguration gridConfig) {
		try {
            SynchronousNamespacePerspective<String, String>    syncNSP;
            
            syncNSP = new DHTClient().openSession(gridConfig)
                    .openSyncNamespacePerspective("_MyNamespace", String.class, String.class);
            syncNSP.put("Hello", "world!");
            return syncNSP.get("Hello");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
	}
    
    public static void main(String[] args) throws IOException {
    	System.out.println( runExample( SKGridConfiguration.parseFile(args[0]) ) );
    }
}
