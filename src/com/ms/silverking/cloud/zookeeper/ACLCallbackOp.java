package com.ms.silverking.cloud.zookeeper;

import java.util.List;

import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

public final class ACLCallbackOp extends AsyncCallbackOp {
    private List<ACL>   acl;
    private Stat        stat;
    
    public ACLCallbackOp() {
        super();
    }
    
    public List<ACL> getACL() {
        return acl;
    }
    
    public Stat getStat() {
        return stat;
    }
    
    public void setComplete(int rc, String path, List<ACL> acl, Stat stat) {
        synchronized (this) {
            this.acl = acl;
            this.stat = stat;
            super.setCompleteInternal(rc, path);
        }
    }
}
