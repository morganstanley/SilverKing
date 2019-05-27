package com.ms.silverking.cloud.dht.client.example;

import java.io.IOException;

import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;

public class HelloObject {        
    
	public static Object runExample(SKGridConfiguration gridConfig) {
		try {
			SynchronousNamespacePerspective<String, Object>    syncNSP;
			
			syncNSP = new DHTClient().openSession(gridConfig)
			        .openSyncNamespacePerspective("_MyNamespace", String.class, Object.class);
			
			syncNSP.put("Hello object", "object world!");
			return syncNSP.get("Hello object");
		} catch (Exception e) {
            throw new RuntimeException(e);
		}
    }
    
    public static void main(String[] args) throws IOException {
    	System.out.println( runExample( SKGridConfiguration.parseFile(args[0]) ) );
    }
}
