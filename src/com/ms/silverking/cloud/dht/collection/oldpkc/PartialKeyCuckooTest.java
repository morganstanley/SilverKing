package com.ms.silverking.cloud.dht.collection.oldpkc;

import java.util.HashMap;
import java.util.Map;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;

/**
 * 
 */
public class PartialKeyCuckooTest {
    private static final boolean    debugCycle = false;
    
    public static void main(String[] args) {
        try {
            OldMD5KeyDigest        digest;
            PartialKeyCuckoo    map;
            Stopwatch   sw;
            Map<DHTKey,Long>    jmap;
            DHTKey[]            key;
            int                 inner;
            int                 outer;
            double              usPerOp;
            long                ops;
            int                 numSubTables;
            int                 entriesPerBucket;
            int                 totalEntries;
            int                 entriesPerSubTable;
            int                 load;
            double              loadFactor;
               
            
            //8/bucket, 2tables, .96 generates errors
            totalEntries = 32768;
            numSubTables = Integer.parseInt(args[0]);
            entriesPerBucket = Integer.parseInt(args[1]);
            loadFactor = Double.parseDouble(args[2]);
            entriesPerSubTable = totalEntries / (numSubTables * entriesPerBucket);
            //entriesPerSubTable = 16384 / entriesPerBucket;
            //totalEntries = numSubTables * entriesPerSubTable * entriesPerBucket;
            
            load = (int)((double)(totalEntries) * loadFactor);
            
            System.out.println("entriesPerSubTable: "+ entriesPerSubTable +"\ttotalEntries: "+ totalEntries);
            System.out.println("loadFactor: "+ loadFactor +"\tload: "+ load);
            
            jmap = new HashMap<>(totalEntries);
            map = new PartialKeyCuckoo(numSubTables, entriesPerBucket, totalEntries, 128);
            
            digest = new OldMD5KeyDigest();
            key = new DHTKey[load];
            for (int i = 0; i < load; i++) {
                key[i] = digest.computeKey((""+ i).getBytes());
            }
            
            outer = 1;
            inner = load;
            sw = new SimpleStopwatch();
            for (int j = 0; j < outer; j++) {
                map.clear();
                for (int i = 0; i < inner; i++) {
                    int     offset;
                    
                    offset = i;
                    if (debugCycle) {
                        System.out.println(key[i]);
                    }
                    map.put(key[i], offset);
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
                    
                    offset = map.get(key[i]);
                    if (offset != i) {
                        System.out.printf("%x\t%d != %d\t%s\n", offset, offset, i, key[i].toString());
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
            
            outer = 10000;
            sw = new SimpleStopwatch();
            for (int j = 0; j < outer; j++) {
                for (int i = 0; i < inner; i++) {
                    long     offset;
                    
                    offset = map.get(key[i]);
                    if (offset != i) {
                        System.out.printf("%x\t%d != %d\t\n", offset, offset, i);
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
