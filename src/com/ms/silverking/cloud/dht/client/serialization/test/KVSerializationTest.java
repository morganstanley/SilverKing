package com.ms.silverking.cloud.dht.client.serialization.test;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

import com.ms.silverking.cloud.dht.client.serialization.internal.SerializedKeyValueBuffers;
import com.ms.silverking.cloud.dht.client.serialization.internal.SerializedKeyValueBuffers.BufferMode;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.SimpleKey;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;

public class KVSerializationTest {
    private static final int    valSize = 8;
    
    public KVSerializationTest() {
    }
    
    public void runTest(int numKeys, BufferMode bufferMode) {
        SerializedKeyValueBuffers   skvBuffers;
        Stopwatch   sw;
        ThreadLocalRandom   random;
        
        skvBuffers = new SerializedKeyValueBuffers();
        random = ThreadLocalRandom.current();
        sw = new SimpleStopwatch();
        for (int i = 0; i < numKeys; i++) {
            DHTKey      key;
            ByteBuffer  value;
            
            key = new SimpleKey(random.nextLong(), random.nextLong());
            value = randomValue(random);
            skvBuffers.addKeyValue(key, value, bufferMode);
        }
        sw.stop();
        System.out.println(skvBuffers);
        System.out.printf("Elapsed %f\n", sw.getElapsedSeconds());
        skvBuffers.freeze();
        display(skvBuffers);
    }
    
    public void display(SerializedKeyValueBuffers skvBuffers) {
        int numKeys;
        
        numKeys = skvBuffers.getNumKeys();
        for (int i = 0; i < numKeys; i++) {
            System.out.printf("%d\t%s\t%s\n", i, skvBuffers.getKey(i).toString(),
                    skvBuffers.getValue(i));
        }
    }
    
    private ByteBuffer randomValue(ThreadLocalRandom random) {
        ByteBuffer  val;
        
        val = ByteBuffer.allocate(valSize);
        random.nextBytes(val.array());
        return val;
    }
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("<numKeys> <bufferMode>");
            return;
        } else {
            int         numKeys;
            BufferMode  bufferMode;
            
            numKeys = Integer.parseInt(args[0]);
            bufferMode = BufferMode.valueOf(args[1]);
            new KVSerializationTest().runTest(numKeys, bufferMode);
        }
    }
}
