package com.ms.silverking.cloud.dht.client.example;

import java.io.IOException;

import com.ms.silverking.cloud.dht.client.AsyncPut;
import com.ms.silverking.cloud.dht.client.AsyncSingleValueRetrieval;
import com.ms.silverking.cloud.dht.client.AsynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;

public class HelloAsyncDHT {    

	public static String runExample(SKGridConfiguration gridConfig) {
		try {
            AsynchronousNamespacePerspective<String, String>    asyncNSP;
            AsyncPut<String>    asyncPut;
            AsyncSingleValueRetrieval<String,String>    asyncGet;
            
            asyncNSP = new DHTClient().openSession(gridConfig)
                    .openAsyncNamespacePerspective("_MyNamespace", String.class, String.class);
            asyncPut = asyncNSP.put("Hello async", "async world!");
            asyncPut.waitForCompletion();
            asyncGet = asyncNSP.get("Hello async");
            asyncGet.waitForCompletion();
            return asyncGet.getValue();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
	}
	
    public static void main(String[] args) throws IOException {
    	System.out.println( runExample( SKGridConfiguration.parseFile(args[0]) ) );
    }
}
