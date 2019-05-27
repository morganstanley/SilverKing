package com.ms.silverking.cloud.dht.client.impl;

import java.util.Comparator;

import com.ms.silverking.cloud.dht.VersionConstraint;

/**
 * Impose an ordering so that compatible retrievals may be grouped together.
 */
class VersionConstraintComparator implements Comparator<VersionConstraint> {
    public static final VersionConstraintComparator  instance = new VersionConstraintComparator();

    @Override
    public int compare(VersionConstraint c1, VersionConstraint c2) {
        int c;
        
        c = c1.getMode().compareTo(c2.getMode());
        if (c != 0) {
            return c;
        } else {
            if (c1.getMin() < c2.getMin()) {
                return -1;
            } else if (c1.getMin() > c2.getMin()) {
                return 1;
            } if (c1.getMax() < c2.getMax()) {
                return -1;
            } else if (c1.getMax() > c2.getMax()) {
                return 1;
            } if (c1.getMaxCreationTime() < c2.getMaxCreationTime()) {
                return -1;
            } else if (c1.getMaxCreationTime() > c2.getMaxCreationTime()) {
                return 1;
            } else {
                return 0;
            }
        }
    }
}
