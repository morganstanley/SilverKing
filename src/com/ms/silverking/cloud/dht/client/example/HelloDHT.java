package com.ms.silverking.cloud.dht.client.example;

import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;

public class HelloDHT {        
    public static void main(String[] args) {
        try {
            SynchronousNamespacePerspective<String, String>    syncNSP;
            
            syncNSP = new DHTClient().openSession(SKGridConfiguration.parseFile(args[0]))
                    .openSyncNamespacePerspective("_MyNamespace", String.class, String.class);
            syncNSP.put("Hello", "world!");
            System.out.println(syncNSP.get("Hello"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
