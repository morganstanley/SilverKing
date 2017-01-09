package com.ms.silverking.cloud.dht.collection.oldpkc;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;

import com.ms.silverking.numeric.NumConversion;
import com.ms.silverking.cloud.dht.collection.HybridPartialKeyCuckoo;
import com.ms.silverking.cloud.dht.collection.PartialKeyIntCuckooBase;
import com.ms.silverking.cloud.dht.collection.WritableCuckooConfig;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;

/**
 * 
 */
public class PartialKeyCuckooTest3 {
    private static final boolean    debugCycle = false;
    
    public static void main(String[] args) {
        try {
            OldMD5KeyDigest        digest;
            PartialKeyIntCuckooBase    map;
            Stopwatch   sw;
            Map<DHTKey,Long>    jmap;
            DHTKey[]            keys;
            int[]               offsets;
            int                 inner;
            int                 outer;
            double              usPerOp;
            long                ops;
            int                 numSubTables;
            int                 entriesPerBucket;
            int                 totalEntries;
            int                 bucketsPerSubTable;
            int                 load;
            double              loadFactor;
            ByteBuffer          segmentBuffer;
            ByteBuffer          htBuffer;
            long[]              htArray;
            
            //8/bucket, 2tables, .96 generates errors
            totalEntries = 32768;
            numSubTables = Integer.parseInt(args[0]);
            entriesPerBucket = Integer.parseInt(args[1]);
            loadFactor = Double.parseDouble(args[2]);
            bucketsPerSubTable = totalEntries / (numSubTables * entriesPerBucket);
            //entriesPerSubTable = 16384 / entriesPerBucket;
            //totalEntries = numSubTables * entriesPerSubTable * entriesPerBucket;
            
            load = (int)((double)(totalEntries) * loadFactor);
            
            System.out.println("bucketsPerSubTable: "+ bucketsPerSubTable +"\ttotalEntries: "+ totalEntries);
            System.out.println("loadFactor: "+ loadFactor +"\tload: "+ load);
            
            segmentBuffer = ByteBuffer.allocate(totalEntries * 2 * NumConversion.BYTES_PER_LONG).order(ByteOrder.LITTLE_ENDIAN);
            htBuffer = ByteBuffer.allocate(totalEntries * NumConversion.BYTES_PER_LONG);
            htArray = new long[totalEntries];  
                        
            digest = new OldMD5KeyDigest();
            keys = new DHTKey[load];
            offsets = new int[keys.length];
            for (int i = 0; i < load; i++) {
                keys[i] = digest.computeKey((""+ i).getBytes());
                offsets[i] = i * 2 * NumConversion.BYTES_PER_LONG;
                segmentBuffer.putLong(offsets[i], keys[i].getMSL());
                segmentBuffer.putLong(offsets[i] + NumConversion.BYTES_PER_LONG, keys[i].getLSL());
                //System.out.printf("%16x %16x\n", keys[i].getMSL(), segmentBuffer.getLong(offsets[i]));
            }
            
            jmap = new HashMap<>(totalEntries);
            //map = new SegmentPartialKeyCuckoo(new WritableCuckooConfig(totalEntries, numSubTables, entriesPerBucket, 8),
            //        segmentBuffer, htBuffer);
            map = new HybridPartialKeyCuckoo(new WritableCuckooConfig(totalEntries, numSubTables, entriesPerBucket, 8),
                    segmentBuffer, htArray, null);
            
            outer = 1;
            inner = load;
            sw = new SimpleStopwatch();
            for (int j = 0; j < outer; j++) {
                map.clear();
                for (int i = 0; i < inner; i++) {
                    if (debugCycle) {
                        System.out.println(keys[i]);
                    }
                    map.put(keys[i], offsets[i]);
                    //jmap.put(key[i], (long)offset);
                    //System.out.println(i +"\t"+ map.get(key, version));
                }
            }
            sw.stop();
            
            ops = inner * outer;
            usPerOp = sw.getElapsedSeconds() / ((double)ops / 1000000.0);
            System.out.println(sw);
            System.out.println(usPerOp);

            System.out.println("warmup");
            sw = new SimpleStopwatch();
            for (int j = 0; j < 100; j++) {
                for (int i = 0; i < inner; i++) {
                    long     offset;
                    
                    offset = map.get(keys[i]);
                    if (offset != offsets[i]) {
                        System.out.printf("%x\t%d != %d\t%s\n", offset, offset, offsets[i], keys[i].toString());
                    }
                    //jmap.put(key[i], (long)offset);
                    //System.out.println(i +"\t"+ map.get(key[i]));
                }
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException ie) {
            }
            System.out.println("warmup complete");
            
            outer = 1000;
            sw = new SimpleStopwatch();
            for (int j = 0; j < outer; j++) {
                for (int i = 0; i < inner; i++) {
                    long     offset;
                    
                    offset = map.get(keys[i]);
                    if (offset != offsets[i]) {
                        System.out.printf("%x\t%d != %d\t%s\n", offset, offset, offsets[i], keys[i].toString());
                    }
                    //jmap.put(key[i], (long)offset);
                    //System.out.println(i +"\t"+ map.get(key[i]));
                }
            }
            sw.stop();
            
            ops = inner * outer;
            usPerOp = sw.getElapsedSeconds() / ((double)ops / 1000000.0);
            System.out.println("\n"+ sw);
            System.out.println("usPerOp\t"+ usPerOp);
            System.out.println();
            map.displaySizes();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
