package com.ms.silverking.collection;

import org.junit.Assert;
import org.junit.Test;

public class TupleTest {
    @Test
    public void testPair() {
        Pair<Integer,Integer>   t;
        
        t = new Pair<>(0, 1);
        for (int i = 0; i < t.getSize(); i++) {
            Assert.assertEquals(i, t.getElement(i));
        }
    }
    
    @Test
    public void testTriple() {
        Triple<Integer,Integer,Integer>   t;
        
        t = new Triple<>(0, 1, 2);
        for (int i = 0; i < t.getSize(); i++) {
            Assert.assertEquals(i, t.getElement(i));
        }
    }
    
    @Test
    public void testQuadruple() {
        Quadruple<Integer,Integer,Integer,Integer>   q;
        
        q = new Quadruple<>(0, 1, 2, 3);
        for (int i = 0; i < q.getSize(); i++) {
            Assert.assertEquals(i, q.getElement(i));
        }
    }
}
