package com.ms.silverking.cloud.dht.collection.oldpkc;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;

/**
 * 
 */
public class PartialKeyCuckooRWTest {
    private static final boolean    debugCycle = false;
    
    private enum Mode {Write, Read};
    
    public static void main(String[] args) {
        try {
            OldMD5KeyDigest        digest;
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
            Mode                mode;
            PKCReaderWriter     pkcRW;
            File                file;
            
            pkcRW = new PKCReaderWriter(new SVTReaderWriter());
            
            //8/bucket, 2tables, .96 generates errors
            totalEntries = 32768;
            mode = Mode.valueOf(args[0]);
            file = new File(args[1]);
            numSubTables = Integer.parseInt(args[2]);
            entriesPerBucket = Integer.parseInt(args[3]);
            loadFactor = Double.parseDouble(args[4]);
            entriesPerSubTable = totalEntries / (numSubTables * entriesPerBucket);
            //entriesPerSubTable = 16384 / entriesPerBucket;
            //totalEntries = numSubTables * entriesPerSubTable * entriesPerBucket;
            
            load = (int)((double)(totalEntries) * loadFactor);
            
            System.out.println("entriesPerSubTable: "+ entriesPerSubTable +"\ttotalEntries: "+ totalEntries);
            System.out.println("loadFactor: "+ loadFactor +"\tload: "+ load);
            
            digest = new OldMD5KeyDigest();
            key = new DHTKey[load];
            for (int i = 0; i < load; i++) {
                key[i] = digest.computeKey((""+ i).getBytes());
            }
            
            jmap = new HashMap<>(totalEntries);
            
            outer = 1;
            inner = load;
            if (mode == Mode.Write) {
                PartialKeyCuckoo    map;
                
                map = new PartialKeyCuckoo(numSubTables, entriesPerBucket, totalEntries, 128);
                
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
                
                pkcRW.write(file, map);
            } else {
                MappedFilePKCReaderWriter     rw;
                MappedFilePartialKeyCuckoo    map;
                
                rw = new MappedFilePKCReaderWriter(new MappedValueTableReaderWriter());
                map = rw.read(file);
                
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
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
