package com.ms.silverking.net.async;

import com.ms.silverking.thread.lwt.LWTCompatibleThread;

public class SelectorThread extends Thread implements LWTCompatibleThread {
    //private boolean allowBlocking;
    
    public SelectorThread(Runnable runnable, String name) {
        super(runnable, name);
        setDaemon(true);
    }
    
    // FUTURE - consider whether this is useful 
    public void run() {
        //LWTThreadUtil.setLWTThread();
        super.run();
    }
    
    /*
    public void setAllowBlocking(boolean allowBlocking) {
        this.allowBlocking = allowBlocking;
    }
    
    public boolean getAllowBlocking() {
        return allowBlocking;
    }
    */

    @Override
    public void setBlocked() {
    }

    @Override
    public void setNonBlocked() {
    }
}
