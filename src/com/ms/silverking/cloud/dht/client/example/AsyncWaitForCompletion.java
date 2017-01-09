package com.ms.silverking.cloud.dht.client.example;


import com.ms.silverking.cloud.dht.client.AsyncSingleValueRetrieval;
import com.ms.silverking.cloud.dht.client.AsynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;

public class AsyncWaitForCompletion {        
    public static void main(String[] args) {
        try {
            AsynchronousNamespacePerspective<String, String>    asyncNSP;
            AsyncSingleValueRetrieval<String,String>    asyncWaitFor;
            
            asyncNSP = new DHTClient().openSession(SKGridConfiguration.parseFile(args[0]))
                    .openAsyncNamespacePerspective("_MyNamespace", String.class, String.class);
            asyncWaitFor = asyncNSP.waitFor("key.1");
            asyncNSP.put("key.1", "value.1");
            System.out.println("Waiting for active ops");
            asyncNSP.waitForActiveOps();
            System.out.println("Wait complete");
            System.out.println(asyncWaitFor.getValue());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
