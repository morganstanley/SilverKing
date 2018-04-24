package com.ms.silverking.cloud.dht.client.example;

import java.io.IOException;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;

public class HelloMap {        
    
	public static Map<String, String> runExample(SKGridConfiguration gridConfig) {
        try {
            SynchronousNamespacePerspective<String, String>    syncNSP;
            Map<String,String>    mapA;
            
            syncNSP = new DHTClient().openSession(gridConfig)
                    .openSyncNamespacePerspective("_POTUS", String.class, String.class);
            
            mapA = ImmutableMap.of("George Washington", "1789-1797", 
                                   "John Adams", "1797-1801",
                                   "Thomas Jefferson", "1801-1809",
                                   "James Madison", "1809-1817",
                                   "James Monroe", "1817-1825");
            syncNSP.put(mapA);
            return syncNSP.get(mapA.keySet());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
	
    public static void main(String[] args) throws IOException {
        Map<String,String> mapB = runExample( SKGridConfiguration.parseFile(args[0]) );
        for (String name : mapB.keySet()) {
            System.out.printf("%-20s %s\n", name, mapB.get(name));
        }
    }
}
