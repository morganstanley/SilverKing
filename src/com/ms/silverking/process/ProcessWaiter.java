package com.ms.silverking.process;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

public class ProcessWaiter implements Runnable {
    private final Process   p;
    private final SynchronousQueue<Integer>  sq;
    
    public static final int TIMEOUT = Integer.MAX_VALUE;
    
    public ProcessWaiter(Process p) {
        this.p = p;
        sq = new SynchronousQueue<Integer>();
        new Thread(this, "ProcessWaiter").start();
    }
    
    public int waitFor(int millis) {
        try {
            Integer result;
            
            result = sq.poll(millis, TimeUnit.MILLISECONDS);
            if (result != null) {
                return result;
            } else {
                return TIMEOUT;
            }
        } catch (InterruptedException ie) {
            throw new RuntimeException("panic");
        }
    }
    
    public void run() {
        try {
            int result;
            
            result = p.waitFor();
            sq.put(result);
        } catch (InterruptedException ie) {
        }
    }
}
