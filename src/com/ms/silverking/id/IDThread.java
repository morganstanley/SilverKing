package com.ms.silverking.id;

public class IDThread extends Thread {
    private final ThreadUUIDState   threadUUIDState;
    
    public IDThread(Runnable runnable) {
        super(runnable);
        threadUUIDState = new ThreadUUIDState();
    }
    
    public ThreadUUIDState getThreadUUIDState() {
        return threadUUIDState;
    }
}
