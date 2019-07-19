package com.ms.silverking.cloud.dht.collection.test;

import com.ms.silverking.cloud.dht.collection.IntArrayCuckoo;
import com.ms.silverking.cloud.dht.collection.TableFullException;
import com.ms.silverking.cloud.dht.collection.WritableCuckooConfig;
import com.ms.silverking.cloud.dht.common.SimpleKey;

public class IntRehashTest {
    private static final int totalEntries = 16;
    private static final int numSubTables = 4;
    private static final int entriesPerBucket = 4;
    private static final int cuckooLimit = 32;    
    
    public static void rehashTest(int size) {
        IntArrayCuckoo  map;
        
        map = new IntArrayCuckoo(new WritableCuckooConfig(totalEntries, numSubTables, entriesPerBucket, cuckooLimit));
        for (int i = 0; i < size; i++) {
            try {
                map.put(new SimpleKey(0, i), i);
            } catch (TableFullException tfe) {
                System.out.println("rehashing");
                map = IntArrayCuckoo.rehashAndAdd(map, new SimpleKey(0, i), i);
            }
        }
    }
    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            int size;
            
            if (args.length != 1) {
                System.err.println("args: <size>");
                return;
            }
            size = Integer.parseInt(args[0]);
            rehashTest(size);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
