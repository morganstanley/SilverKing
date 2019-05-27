package com.ms.silverking.cloud.zookeeper;

import java.util.List;

import org.apache.zookeeper.data.Stat;

public final class Children2CallbackOp extends AsyncCallbackOp {
    private List<String>    children;
    private Stat            stat;
    
    public Children2CallbackOp() {
        super();
    }
    
    public List<String> getChildren() {
        return children;
    }
    
    public Stat getStat() {
        return stat;
    }
    
    public void setComplete(int rc, String path, List<String> children, Stat stat) {
        synchronized (this) {
            this.children = children;
            this.stat = stat;
            super.setCompleteInternal(rc, path);
        }
    }
}
