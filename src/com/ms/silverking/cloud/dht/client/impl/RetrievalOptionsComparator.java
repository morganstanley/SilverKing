package com.ms.silverking.cloud.dht.client.impl;

import java.util.Comparator;

import com.ms.silverking.cloud.dht.RetrievalOptions;

/**
 * Impose an ordering so that compatible retrievals may be grouped together.
 */
class RetrievalOptionsComparator implements Comparator<RetrievalOptions> {
    public static RetrievalOptionsComparator    instance = new RetrievalOptionsComparator();
    
    @Override
    public int compare(RetrievalOptions o1, RetrievalOptions o2) {
        int c;
        
        c = o1.getRetrievalType().compareTo(o2.getRetrievalType());
        if (c != 0) {
            return c;
        } else {
            c = o1.getWaitMode().compareTo(o2.getWaitMode());
            if (c != 0) {
                return c;
            } else {
                c = o1.getNonExistenceResponse().compareTo(o2.getNonExistenceResponse());
                if (c != 0) {
                    return c;
                } else {
                    c = VersionConstraintComparator.instance.compare(o1.getVersionConstraint(), o2.getVersionConstraint());
                    if (c != 0) {
                        return c;
                    } else {
                        c = Boolean.compare(o1.getVerifyChecksums(), o2.getVerifyChecksums());
                        if (c != 0) {
                            return c;
                        } else {
                            return 0;
                        }
                    }
                }
            }
        }
    }
}
