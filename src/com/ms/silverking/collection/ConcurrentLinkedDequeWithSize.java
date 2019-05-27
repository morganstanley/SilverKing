package com.ms.silverking.collection;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A subset of ConcurrentLinkedDeque functionality with a constant time size function.
 * Note that size is only loosely consistent with the actual size at any given moment. 
 * 
 * Exceptions in the underlying deque may cause the reported size to diverge from reality.
 * This class is intended for the case where such as exception will either not occur, or be fatal.
 */
public class ConcurrentLinkedDequeWithSize<E> {
	private final ConcurrentLinkedDeque<E>	dq;
	private final AtomicInteger				size;
	
	public ConcurrentLinkedDequeWithSize() {
		dq = new ConcurrentLinkedDeque();
		size = new AtomicInteger();
	}
	
    public E poll() { 
    	E	e;
    	
    	e = dq.poll();
    	if (e != null) {
        	size.decrementAndGet();
    	}
    	return e;
    }
    
    public E remove() {
    	size.decrementAndGet();
    	return dq.remove(); 
    }
    
    public E peek() {
    	return dq.peek();
    }
    
    public void push(E e) {
    	size.incrementAndGet();
    	dq.push(e); 
    }
    
    public boolean isEmpty() {
    	return dq.isEmpty();
    }
	
    public boolean add(E e) {
    	size.incrementAndGet();
    	return dq.add(e);
    }
    
	public int size() {
		return size.get();
	}
}
