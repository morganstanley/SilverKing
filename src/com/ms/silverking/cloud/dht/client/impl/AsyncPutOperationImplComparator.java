package com.ms.silverking.cloud.dht.client.impl;

import java.util.Comparator;

/**
 * This class imposes an ordering on instances that are being ordered for sending. 
 * We want to send earlier versions first. Other ordering is arbitrary but intended to
 * group compatible operations together.
 */
class AsyncPutOperationImplComparator implements Comparator<AsyncPutOperationImpl> {
    public static final AsyncPutOperationImplComparator  instance = new AsyncPutOperationImplComparator();
    
    @Override
    public int compare(AsyncPutOperationImpl o1, AsyncPutOperationImpl o2) {
        if (o1.getPotentiallyUnresolvedVersion() < o2.getPotentiallyUnresolvedVersion()) {
            return -1;
        } else if (o1.getPotentiallyUnresolvedVersion() > o2.getPotentiallyUnresolvedVersion()) {
            return 1;
        } else {
            return PutOptionsComparator.instance.compare(o1.putOptions(), o2.putOptions());
        }
    }
}
