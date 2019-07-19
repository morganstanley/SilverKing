package com.ms.silverking.util.concurrent.locks;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * User-level spin lock. Use with care as the scheduler may preempt the thread
 * that holds the lock at any time.
 */
public class SpinLock implements Lock {
    private final AtomicBoolean locked;
    
    public SpinLock() {
        locked = new AtomicBoolean();
    }

    @Override
    public void lock() {
        while (!locked.compareAndSet(false, true));
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryLock() {
        return locked.compareAndSet(false, true);
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        long    timeNanos;
        long    t1;
        boolean acquired;
        
        t1 = System.nanoTime();
        timeNanos = unit.convert(time, TimeUnit.NANOSECONDS);
        do {
            acquired = locked.compareAndSet(false, true);
        } while (!acquired && System.nanoTime() - t1 < timeNanos);
        return acquired;
    }

    @Override
    public void unlock() {
        locked.getAndSet(false);
        // consider owner verification
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }
}
