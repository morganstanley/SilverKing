package com.ms.silverking.cloud.dht.client.example;

import com.ms.silverking.cloud.dht.client.AsyncPut;
import com.ms.silverking.cloud.dht.client.AsyncSingleValueRetrieval;
import com.ms.silverking.cloud.dht.client.AsynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;

public class HelloAsyncDHT {        
    public static void main(String[] args) {
        try {
            AsynchronousNamespacePerspective<String, String>    asyncNSP;
            AsyncPut<String>    asyncPut;
            AsyncSingleValueRetrieval<String,String>    asyncGet;
            
            asyncNSP = new DHTClient().openSession(SKGridConfiguration.parseFile(args[0]))
                    .openAsyncNamespacePerspective("_MyNamespace", String.class, String.class);
            asyncPut = asyncNSP.put("Hello async", "async world!");
            asyncPut.waitForCompletion();
            asyncGet = asyncNSP.get("Hello async");
            asyncGet.waitForCompletion();
            System.out.println(asyncGet.getValue());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
