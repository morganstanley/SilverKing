package com.ms.silverking.cloud.zookeeper;

import org.apache.zookeeper.data.Stat;

public final class DataCallbackOp extends AsyncCallbackOp {
    private byte[]  data;
    private Stat    stat;
    
    public DataCallbackOp() {
        super();
    }
    
    public byte[] getData() {
        return data;
    }
    
    public Stat getStat() {
        return stat;
    }
    
    public void setComplete(int rc, String path, byte[] data, Stat stat) {
        synchronized (this) {
            this.data = data;
            this.stat = stat;
            super.setCompleteInternal(rc, path);
        }
    }
}
