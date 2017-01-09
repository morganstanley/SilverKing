package com.ms.silverking.cloud.dht.client.example;


import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;

public class HelloByteArray {        
    public static void main(String[] args) {
        try {
            SynchronousNamespacePerspective<String, byte[]>    syncNSP;
            
            syncNSP = new DHTClient().openSession(SKGridConfiguration.parseFile(args[0]))
                    .openSyncNamespacePerspective("_MyNamespace", String.class, byte[].class);
            
            syncNSP.put("Hello byte[]", "byte[] world!".getBytes());
            System.out.println(new String(syncNSP.get("Hello byte[]")));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
