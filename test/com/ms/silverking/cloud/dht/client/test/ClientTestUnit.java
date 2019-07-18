package com.ms.silverking.cloud.dht.client.test;

import java.io.IOException;
import java.util.List;

import org.junit.Test;

import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.log.Log;
import com.ms.silverking.testing.annotations.SkLarge;

@SkLarge
public class ClientTestUnit {
    public void runTests(String gcName) throws IOException, ClientException {
        ClientTestFramework tf;
        List<Triple<String,Integer,Integer>>    results;
        
        tf = new ClientTestFramework(gcName);
        results = tf.runTests(ClientTestRegistry.getAllTests());
        Log.warning("\n** Test results **");
        for (Triple<String,Integer,Integer> result : results) {
            System.out.printf("%s\t%d\t%d\n", result.getV1(), result.getV2(), result.getV3());
        }        
    }
    
    @Test
    public void runTestsEmbedded() throws IOException, ClientException {
        runTests(null);
    }
}
