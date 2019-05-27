package com.ms.silverking.cloud.zookeeper;

import java.util.concurrent.TimeUnit;

import com.ms.silverking.log.Log;

public abstract class AsyncCallbackOp {
    private final ZKRequestUUID    zkUUID;
    private State   state;
    private int     rc;
    private String  path;
    
    private enum State  {incomplete, complete};
    
    public AsyncCallbackOp() {
        zkUUID = new ZKRequestUUID();
        state = State.incomplete;
    }
    
    public ZKRequestUUID getRequestUUID() {
        return zkUUID;
    }
    
    public int getRC() {
        return rc;
    }
    
    public String getPath() {
        return path;
    }
    
    protected void setCompleteInternal(int rc, String path) {
        if (state == State.incomplete) {
            this.rc = rc;
            this.path = path;
            state = State.complete;
            this.notifyAll();
        } else {
            Log.warning("Ignoring multiple completions for: ", zkUUID);
        }
    }
    
    public void waitForCompletion() {
        synchronized (this) {
            while (state == State.incomplete) {
                try {
                    this.wait();
                } catch (InterruptedException ie) {
                }
            }
        }
    }
    
    public void waitForCompletion(long timeout, TimeUnit unit) {
        long    timeoutMillis;
        
        timeoutMillis = TimeUnit.MILLISECONDS.convert(timeout, unit);
        synchronized (this) {
            while (state == State.incomplete) {
                try {
                    this.wait(timeoutMillis);
                } catch (InterruptedException ie) {
                }
            }
        }
    }
}
