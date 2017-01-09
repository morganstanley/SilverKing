package com.ms.silverking.cloud.dht.client.example;

import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;

public class HelloObject {        
    public static void main(String[] args) {
        try {
            SynchronousNamespacePerspective<String, Object>    syncNSP;
            
            syncNSP = new DHTClient().openSession(SKGridConfiguration.parseFile(args[0]))
                    .openSyncNamespacePerspective("_MyNamespace", String.class, Object.class);
            
            syncNSP.put("Hello object", "object world!");
            System.out.println(syncNSP.get("Hello object"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
