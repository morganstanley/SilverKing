package com.ms.silverking.collection;

import org.jgrapht.util.FibonacciHeap;
import org.jgrapht.util.FibonacciHeapNode;

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class FibPriorityQueue<T> extends AbstractQueue<T> {
  private FibonacciHeap<T> heap = new FibonacciHeap<>();

  @Override
  public boolean add(T e) {
    return false;
  }

  public boolean add(T e, Double k) {
    FibonacciHeapNode<T> n = new FibonacciHeapNode<>(e);
    heap.insert(n, k);
    return true;
  }

  @Override
  public boolean offer(T e) {
    return add(e);
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException();
  }

  private class FibPriorityQueueIterator<T> implements Iterator<T> {
    FibPriorityQueue<T> pq;

    public FibPriorityQueueIterator(FibPriorityQueue<T> givenPq) {
      this.pq = givenPq;
    }

    @Override
    public boolean hasNext() {
      return !this.pq.heap.isEmpty();
    }

    @Override
    public T next() {
      T nxt = this.pq.poll();
      if (nxt == null) {
        throw new NoSuchElementException();
      } else {
        return nxt;
      }
    }
  }

  @Override
  public Iterator<T> iterator() {
    return new FibPriorityQueueIterator<>(this);
  }

  @Override
  public int size() {
    return heap.size();
  }

  @Override
  public T poll() {
    if (heap.min() == null) {
      return null;
    } else {
      return heap.removeMin().getData();
    }
  }

  @Override
  public T peek() {
    if (heap.min() == null) {
      return null;
    } else {
      return heap.min().getData();
    }
  }
}
