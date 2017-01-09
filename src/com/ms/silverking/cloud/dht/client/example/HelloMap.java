package com.ms.silverking.cloud.dht.client.example;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;

public class HelloMap {        
    public static void main(String[] args) {
        try {
            SynchronousNamespacePerspective<String, String>    syncNSP;
            Map<String,String>    mapA;
            Map<String,String>    mapB;
            
            syncNSP = new DHTClient().openSession(SKGridConfiguration.parseFile(args[0]))
                    .openSyncNamespacePerspective("_POTUS", String.class, String.class);
            
            mapA = ImmutableMap.of("George Washington", "1789-1797", 
                                   "John Adams", "1797-1801",
                                   "Thomas Jefferson", "1801-1809",
                                   "James Madison", "1809-1817",
                                   "James Monroe", "1817-1825");
            syncNSP.put(mapA);
            mapB = syncNSP.get(mapA.keySet());
            for (String name : mapB.keySet()) {
                System.out.printf("%-20s %s\n", name, mapB.get(name));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
