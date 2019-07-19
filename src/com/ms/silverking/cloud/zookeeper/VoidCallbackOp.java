package com.ms.silverking.cloud.zookeeper;


public final class VoidCallbackOp extends AsyncCallbackOp {
    public VoidCallbackOp() {
        super();
    }
    
    public void setComplete(int rc, String path) {
        synchronized (this) {
            super.setCompleteInternal(rc, path);
        }
    }
}
