package com.ms.silverking.cloud.dht;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.ms.silverking.cloud.dht.common.DHTConstants;

/**
 * Time that value was created.
 */
public class CreationTime implements Comparable<CreationTime> {
    private final long  creationTimeNanos;
    
    private static final long    nanoOriginTimeInMillis = DHTConstants.nanoOriginTimeInMillis;
    private static final SimpleDateFormat	sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss.SSS zzz yyyy");
    
    private static final long   nanosPerMilli = 1000000;
    
    public CreationTime(long creationTimeNanos) {
        this.creationTimeNanos = creationTimeNanos;
    }
    
    public long inNanos() {
        return creationTimeNanos;
    }
    
    public long inMillis() {
        return creationTimeNanos / nanosPerMilli + nanoOriginTimeInMillis;
    }
    
    @Override
    public int hashCode() {
        return (int)creationTimeNanos;
    }
    
    @Override
    public boolean equals(Object o) {
        CreationTime    oct;
        
        oct = (CreationTime)o;
        return creationTimeNanos == oct.creationTimeNanos;
    }

    @Override
    public int compareTo(CreationTime o) {
        if (creationTimeNanos < o.creationTimeNanos) {
            return -1;
        } else if (creationTimeNanos > o.creationTimeNanos) {
            return 1;
        } else {
            return 0;
        }
    }
    
    @Override
    public String toString() {
        return Long.toString(creationTimeNanos);
    }

    public String toDateString() {
        return sdf.format(new Date(inMillis()));    	
    }
}
