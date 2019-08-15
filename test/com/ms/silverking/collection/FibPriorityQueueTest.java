package com.ms.silverking.collection;

import org.junit.Ignore;
import org.junit.Test;

import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;

public class FibPriorityQueueTest {

    @Test
    public void testRemoveInOrder() {
        FibPriorityQueue<Double> fpq = new FibPriorityQueue<>();
        fpq.add(1D, 1D);
        fpq.add(3D, 3D);
        fpq.add(2D, 2D);
        fpq.add(4D, 4D);
        fpq.add(-1D, -1D);
        fpq.add(-3D, -3D);
        fpq.add(-2D, -2D);
        fpq.add(-4D, -4D);
        assertEquals(new Double(-4), fpq.remove());
        assertEquals(new Double(-3), fpq.remove());
        assertEquals(new Double(-2), fpq.remove());
        assertEquals(new Double(-1), fpq.remove());
        assertEquals(new Double(1), fpq.remove());
        assertEquals(new Double(2), fpq.remove());
        assertEquals(new Double(3), fpq.remove());
        assertEquals(new Double(4), fpq.remove());
    }

    @Test
    public void testPeek() {
        FibPriorityQueue<Long> fpq = new FibPriorityQueue<>();
        assertEquals(null, fpq.peek());
        fpq.add(7L, 1D);
        assertEquals(new Long(7), fpq.peek());
        fpq.add(6L, 2D);
        assertEquals(new Long(7), fpq.peek());
    }

    @Test
    public void testPoll() {
        FibPriorityQueue<String> fpq = new FibPriorityQueue<>();
        assertEquals(null, fpq.poll());
        fpq.add("abc", 1D);
        assertEquals("abc", fpq.poll());
        fpq.add("def", -1D);
        assertEquals("def", fpq.poll());
    }

    @Test(expected = NoSuchElementException.class)
    public void testEmptyIterator() {
        FibPriorityQueue<Long> fpq = new FibPriorityQueue<>();
        Iterator<Long> emptyIt = fpq.iterator();
        emptyIt.next();
    }

    @Test
    public void testIterator() {
        FibPriorityQueue<Long> fpq = new FibPriorityQueue<>();
        assertEquals(false, fpq.iterator().hasNext());
        fpq.add(1L, 1D);
        fpq.add(2L, 2D);
        fpq.add(3L, 3D);
        fpq.add(4L, 4D);
        Iterator<Long> it = fpq.iterator();
        assertEquals(true, it.hasNext());
        assertEquals(new Long(1), it.next());
        assertEquals(true, it.hasNext());
        assertEquals(new Long(2), it.next());
        assertEquals(true, it.hasNext());
        assertEquals(new Long(3), it.next());
        assertEquals(true, it.hasNext());
        assertEquals(new Long(4), it.next());
        assertEquals(false, it.hasNext());
    }

    @Test
    public void testSize() {
        FibPriorityQueue<String> fpq = new FibPriorityQueue<>();
        assertEquals(0, fpq.size());
        fpq.add("abcd", 1D);
        assertEquals(1, fpq.size());
        fpq.add("efgh", 2D);
        assertEquals(2, fpq.size());
        fpq.remove();
        assertEquals(1, fpq.size());
        fpq.remove();
        assertEquals(0, fpq.size());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemove() {
        FibPriorityQueue<String> fpq = new FibPriorityQueue<>();
        fpq.add("a", 1D);
        fpq.add("b", 2D);
        fpq.add("c", 3D);
        fpq.add("d", 4D);
        fpq.add("e", 5D);
        assertEquals(5, fpq.size());
        fpq.remove("a");
    }
}
