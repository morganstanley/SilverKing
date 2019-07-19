package com.ms.silverking.cloud.dht.daemon;

/**
 * Tells the RingMaster what type of replicas a query is after. This is important
 * during ring transitions.  
 */
public enum RingOwnerQueryOpType {
    Write, Read;
}