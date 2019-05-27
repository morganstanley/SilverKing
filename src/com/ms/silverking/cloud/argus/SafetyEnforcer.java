package com.ms.silverking.cloud.argus;

/**
 * Enforces a particular safety constraint. 
 */
public interface SafetyEnforcer {
    /**
     * @return milliseconds until next check
     */
    public int enforce();
}
