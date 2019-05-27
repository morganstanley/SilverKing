package com.ms.silverking.cloud.dht.client.example;


import java.io.IOException;

import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;

public class HelloByteArray {        
    
	public static byte[] runExample(SKGridConfiguration gridConfig) {
        try {
            SynchronousNamespacePerspective<String, byte[]>    syncNSP;
            
            syncNSP = new DHTClient().openSession(gridConfig)
                    .openSyncNamespacePerspective("_MyNamespace", String.class, byte[].class);
            
            syncNSP.put("Hello byte[]", "byte[] world!".getBytes());
            return syncNSP.get("Hello byte[]");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
	
    public static void main(String[] args) throws IOException {
    	System.out.println( runExample( SKGridConfiguration.parseFile(args[0]) ) );
    }
}
