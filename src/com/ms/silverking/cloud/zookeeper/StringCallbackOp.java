package com.ms.silverking.cloud.zookeeper;


public final class StringCallbackOp extends AsyncCallbackOp {
    private String  name;
    
    public StringCallbackOp() {
        super();
    }
    
    public String getName() {
        return name;
    }
    
    public void setComplete(int rc, String path, String name) {
        synchronized (this) {
            this.name = name;
            super.setCompleteInternal(rc, path);
        }
    }
}
