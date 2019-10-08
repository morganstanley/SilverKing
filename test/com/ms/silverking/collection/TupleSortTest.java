package com.ms.silverking.collection;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Assert;
import org.junit.Test;

public class TupleSortTest {
    private static final int    listSize = 10;
    
    @Test
    public void testSort() {
        List<Pair<Integer,Integer>> pairs;
        int prev;
        
        pairs = new ArrayList<>();
        for (int i = 0; i < listSize; i++) {
            Pair<Integer,Integer>   pair;
            
            pair = new Pair<>(0, ThreadLocalRandom.current().nextInt());
            pairs.add(pair);
            System.out.printf("%s\n", pair);
        }
        TupleUtil.sort(pairs, 1, IntegerComparator.instance);
        
        System.out.println();
        prev = Integer.MIN_VALUE;
        for (Pair<Integer,Integer> pair : pairs) {
            Assert.assertTrue(pair.getV2() >= prev);
            System.out.printf("%s\n", pair);
        }
    }
}
