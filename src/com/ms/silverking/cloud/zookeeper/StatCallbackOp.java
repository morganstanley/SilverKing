package com.ms.silverking.cloud.zookeeper;

import org.apache.zookeeper.data.Stat;

public final class StatCallbackOp extends AsyncCallbackOp {
    private Stat    stat;
    
    public StatCallbackOp() {
        super();
    }
    
    public Stat getStat() {
        return stat;
    }
    
    public void setComplete(int rc, String path, Stat stat) {
        synchronized (this) {
            this.stat = stat;
            super.setCompleteInternal(rc, path);
        }
    }
}
