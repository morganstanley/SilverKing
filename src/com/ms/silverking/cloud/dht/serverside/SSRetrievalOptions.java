package com.ms.silverking.cloud.dht.serverside;

import com.ms.silverking.cloud.dht.RetrievalType;
import com.ms.silverking.cloud.dht.VersionConstraint;

public interface SSRetrievalOptions {
    public boolean getVerifyIntegrity();
    public RetrievalType getRetrievalType();
    public VersionConstraint getVersionConstraint();
    public boolean getReturnInvalidations();
}
