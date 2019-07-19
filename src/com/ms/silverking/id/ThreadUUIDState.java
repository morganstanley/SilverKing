package com.ms.silverking.id;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class ThreadUUIDState {
    public final long          longMSB;
    //private final MutableLong   nextLongLSB;
    private final AtomicLong   nextLongLSB;
    
    ThreadUUIDState() {
        UUID    randomUUID;
        
        randomUUID = UUID.randomUUID();
        longMSB = randomUUID.getMostSignificantBits();
        //nextLongLSB = new MutableLong(randomUUID.getLeastSignificantBits());
        nextLongLSB = new AtomicLong(randomUUID.getLeastSignificantBits());
    }
    
    long getNextLongLSB() {
        return nextLongLSB.getAndIncrement();
    }
}
