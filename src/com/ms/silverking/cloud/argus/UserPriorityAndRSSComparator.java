package com.ms.silverking.cloud.argus;

import java.util.Set;

import com.ms.silverking.os.linux.proc.ProcessStat;
import com.ms.silverking.os.linux.proc.ProcessStatAndOwner;

/**
 * Compares candidates for termination.
 * Return in increasing order of termination preference. 
 */


public class UserPriorityAndRSSComparator implements RSSCandidateComparator {
    private final Set<String>   prioritizedUserPatterns;
    private final int           rssPrioritizationThreshold;
    
    public UserPriorityAndRSSComparator(Set<String> prioritizedUserPatterns, int rssPrioritizationThreshold) {
        this.prioritizedUserPatterns = prioritizedUserPatterns;
        this.rssPrioritizationThreshold = rssPrioritizationThreshold;
    }
    
    /**
     * Compares candidates for termination. 
     * Return in increasing order of termination preference. 
     * @return -1 if pso2 should be terminated before pso1.
     * 1 if pso1 should be terminated before pso2. 0 if they are of equal termination
     * priority.   
     */
    @Override
    public int compare(ProcessStatAndOwner pso1, ProcessStatAndOwner pso2) {
        if (pso1.getStat().getRSSBytes() < rssPrioritizationThreshold) {
            if (pso2.getStat().getRSSBytes() < rssPrioritizationThreshold) {
                // both are below threshold; check users and RSS
                return compareUsersAndRSS(pso1, pso2);
            } else {
                return -1;
            }
        } else {
            if (pso2.getStat().getRSSBytes() < rssPrioritizationThreshold) {
                return 1;
            } else {
                // neither is below threshold; check users and RSS
                return compareUsersAndRSS(pso1, pso2);
            }
        }
    }
    
    private int compareUsersAndRSS(ProcessStatAndOwner pso1,
            ProcessStatAndOwner pso2) {
        if (ownedByPrioritizedUser(pso1)) {
            if (ownedByPrioritizedUser(pso2)) {
                // both are prioritized; compare RSS only
                return compareRSS(pso1.getStat(), pso2.getStat());
            } else {
                return -1;
            }
        } else {
            if (ownedByPrioritizedUser(pso2)) {
                return 1;
            } else {
                // neither is prioritized; compare RSS only
                return compareRSS(pso1.getStat(), pso2.getStat());
            }
        }
    }
    
    private boolean ownedByPrioritizedUser(ProcessStatAndOwner pso) {
        for (String pattern : prioritizedUserPatterns) {
            if (pso.getOwner().matches(pattern)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Compare for termination solely on RSS. We prefer
     * to terminate the process with larger RSS
     * @param s1
     * @param s2
     * @return -1 if s1 rss < s2 rss, 1 if s1 rss > s2 rss, 0 otherwise
     */
    private int compareRSS(ProcessStat s1, ProcessStat s2) {
        if (s1.getRSSBytes() < s2.getRSSBytes()) {
            return -1;
        } else if (s1.getRSSBytes() > s2.getRSSBytes()) {
            return 1;
        } else {
            return 0;
        }
    }
}
