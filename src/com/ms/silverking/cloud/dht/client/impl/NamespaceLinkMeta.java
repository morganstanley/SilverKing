package com.ms.silverking.cloud.dht.client.impl;

import com.ms.silverking.cloud.dht.client.NamespaceLinkException;
import com.ms.silverking.cloud.dht.meta.NamespaceLinksZK;

public class NamespaceLinkMeta {
    private final NamespaceLinksZK  nsLinksZK;
    
    public NamespaceLinkMeta(NamespaceLinksZK nsLinksZK) {
        this.nsLinksZK = nsLinksZK;
    }
    
    public void createLink(String child, String parent) throws NamespaceLinkException {
        try {
            nsLinksZK.writeToZK(child, parent);
        } catch (Exception e) {
            throw new NamespaceLinkException(e);
        }
    }
}
