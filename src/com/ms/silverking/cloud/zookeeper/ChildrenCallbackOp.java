package com.ms.silverking.cloud.zookeeper;

import java.util.List;

public final class ChildrenCallbackOp extends AsyncCallbackOp {
    private List<String>    children;
    
    public ChildrenCallbackOp() {
        super();
    }
    
    public List<String> getChildren() {
        return children;
    }
    
    public void setComplete(int rc, String path, List<String> children) {
        synchronized (this) {
            this.children = children;
            super.setCompleteInternal(rc, path);
        }
    }
}
