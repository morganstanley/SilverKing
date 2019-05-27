package com.ms.silverking.cloud.argus;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ms.silverking.os.linux.proc.ProcessStat;
import com.ms.silverking.os.linux.proc.ProcessStatAndOwner;

/**
 * Compares candidates for termination.
 * Return in increasing order of termination preference.
 * 
 * Group RSS by user.
 */
public class UserPriorityAndUserRSSComparator implements RSSCandidateComparator {
    private final Set<String>   prioritizedUserPatterns;
    private final int           rssPrioritizationThreshold;
    private final Map<String,Long>   userRSS;
    
    public UserPriorityAndUserRSSComparator(Set<String> prioritizedUserPatterns, int rssPrioritizationThreshold,
                                            List<ProcessStatAndOwner> procStatsAndOwners) {
        this.prioritizedUserPatterns = prioritizedUserPatterns;
        this.rssPrioritizationThreshold = rssPrioritizationThreshold;
        userRSS = createUserRSS(procStatsAndOwners);
    }
    
    private static Map<String,Long> createUserRSS(List<ProcessStatAndOwner> procStatsAndOwners) {
        Map<String,Long> userRSS;
        
        userRSS = new HashMap<>();
        for (ProcessStatAndOwner procStatAndOwner : procStatsAndOwners) {
            if (procStatAndOwner.getOwner() != null) {
                Long    totalRSS;
                
                totalRSS = userRSS.get(procStatAndOwner.getOwner());
                if (totalRSS == null) {
                    totalRSS = 0L;
                }
                totalRSS += procStatAndOwner.getStat().getRSSBytes();
                userRSS.put(procStatAndOwner.getOwner(), totalRSS);
            }
        }
        return userRSS;
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
                return compareRSS(pso1, pso2);
            } else {
                return -1;
            }
        } else {
            if (ownedByPrioritizedUser(pso2)) {
                return 1;
            } else {
                // neither is prioritized; compare RSS only
                return compareRSS(pso1, pso2);
            }
        }
    }
    
    private boolean ownedByPrioritizedUser(ProcessStatAndOwner pso) {
        for (String pattern : prioritizedUserPatterns) {
            if (pso.getOwner() != null && pso.getOwner().matches(pattern)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Compare for termination by RSS. First compare total user RSS. If that is equal,
     * then look at process RSS. 
     * @param pso1
     * @param pso2
     * @return -1 if pso1 rss < pso2 rss, 1 if pso1 rss > pso2 rss, 0 otherwise
     */
    private int compareRSS(ProcessStatAndOwner pso1, ProcessStatAndOwner pso2) {
        int res;
        
        res = compareTotalUserRSS(pso1, pso2);
        if (res != 0) {
            return res;
        } else {
            return compareProcessRSS(pso1.getStat(), pso2.getStat());
        }
    }
    
    /**
     * Compare for termination by total user RSS. We prefer
     * to terminate the process with larger RSS
     * @param pso1
     * @param pso2
     * @return -1 if pso1 rss < pso2 rss, 1 if pso1 rss > pso2 rss, 0 otherwise
     */
    private int compareTotalUserRSS(ProcessStatAndOwner pso1, ProcessStatAndOwner pso2) {
        if (userRSS.get(pso1.getOwner()) < userRSS.get(pso2.getOwner())) {
            return -1;
        } else if (userRSS.get(pso1.getOwner()) > userRSS.get(pso2.getOwner())) {
            return 1;
        } else {
            return 0;
        }
    }
    
    /**
     * Compare for termination solely on process RSS. We prefer
     * to terminate the process with larger RSS
     * @param s1
     * @param s2
     * @return -1 if s1 rss < s2 rss, 1 if s1 rss > s2 rss, 0 otherwise
     */
    private int compareProcessRSS(ProcessStat s1, ProcessStat s2) {
        if (s1.getRSSBytes() < s2.getRSSBytes()) {
            return -1;
        } else if (s1.getRSSBytes() > s2.getRSSBytes()) {
            return 1;
        } else {
            return 0;
        }
    }
}
