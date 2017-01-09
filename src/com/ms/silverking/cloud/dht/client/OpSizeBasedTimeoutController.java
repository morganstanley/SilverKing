package com.ms.silverking.cloud.dht.client;

import com.ms.silverking.cloud.dht.client.impl.SegmentationUtil;
import com.ms.silverking.text.ObjectDefParser2;

/**
 * An OpTimeoutController that computes timeouts based on operation size.
 * Specifically, each attempt timeout is computed as:
 * constantTimeMillis + itemTimeMillis * opSizeInKeys. For non-keyed
 * operations, the nonKeyedOpMaxRelTimeout_ms is used.
 */
public class OpSizeBasedTimeoutController implements OpTimeoutController {
    private final int   maxAttempts;
    private final int   constantTime_ms;
    private final int   itemTime_ms;
    //private final int   keyedOpMaxRelTimeout_ms;
    private final int   nonKeyedOpMaxRelTimeout_ms;
    
    private static final long    minTransferRate_bps = 275 * 1000 * 1000;
    private static final long    minTransferRate_Bps = minTransferRate_bps / 8;
    private static final int    defaultItemTime_ms = Math.toIntExact((1000L * SegmentationUtil.maxValueSegmentSize) 
                                                     / minTransferRate_Bps);
    
    // For testing
    //private static final int    defaultConstantTime_ms = 10 * 1000;
    //private static final int    defaultConstantTime_ms = 20 * 1000;
    
    // For production
    private static final int    defaultConstantTime_ms = 30 * 1000;
    
    private static final int    defaultMaxAttempts = 6;
    
    private static final OpSizeBasedTimeoutController   template = new OpSizeBasedTimeoutController();
    
    private static final int	defaultKeyedOpMaxRelTimeout_ms = 10 * 60 * 1000;
    private static final int	defaultNonKeyedOpMaxRelTimeout_ms = 2 * 60 * 1000;
    
    static {
    	try {
    		ObjectDefParser2.addParser(template);
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    }    
    
    /*
     * Temporarily removing keyedOpMaxRelTimeout_ms. Add back in when we have time to
     * push this into C++.
     */
    
    /**
     * Construct a fully-specified OpSizeBasedTimeoutController
     * @param maxAttempts maximum number of attempts
     * @param constantTimeMillis constant time in milliseconds 
     * @param itemTimeMillis per-item time in milliseconds
     * @param nonKeyedOpMaxRelTimeout_ms maximum relative timeout in milliseconds
     */
    public OpSizeBasedTimeoutController(int maxAttempts, int constantTimeMillis, int itemTimeMillis,
    									/*int keyedOpMaxRelTimeout_ms,*/ int nonKeyedOpMaxRelTimeout_ms) {
    	if (maxAttempts < min_maxAttempts) {
    		throw new RuntimeException("maxAttempts < min_maxAttempts; "+ maxAttempts +" < "+ min_maxAttempts);
    	}
        this.maxAttempts = maxAttempts;
        this.constantTime_ms = constantTimeMillis;
        this.itemTime_ms = itemTimeMillis;
        //this.keyedOpMaxRelTimeout_ms = keyedOpMaxRelTimeout_ms;
        this.nonKeyedOpMaxRelTimeout_ms = nonKeyedOpMaxRelTimeout_ms;
    }
    
    /**
     * Construct an OpSizeBasedTimeoutController using default parameters
     */
    public OpSizeBasedTimeoutController() {
        this(defaultMaxAttempts, defaultConstantTime_ms, defaultItemTime_ms, /*defaultKeyedOpMaxRelTimeout_ms, */defaultNonKeyedOpMaxRelTimeout_ms);
    }
    
    @Override
    public int getMaxAttempts(AsyncOperation op) {
        return maxAttempts;
    }
    
    @Override
    public int getRelativeTimeoutMillisForAttempt(AsyncOperation op, int attemptIndex) {
        if (op instanceof AsyncKeyedOperation) {
            return computeTimeout(((AsyncKeyedOperation)op).getNumKeys());
        } else {
            return nonKeyedOpMaxRelTimeout_ms;
        }
    }

    @Override
    public int getMaxRelativeTimeoutMillis(AsyncOperation op) {
        if (op instanceof AsyncKeyedOperation) {
            return getRelativeTimeoutMillisForAttempt(op, maxAttempts) * maxAttempts;
        } else {
            return nonKeyedOpMaxRelTimeout_ms;
        }
    }
    
    private int computeTimeout(int numItems) {
        //return Math.min(constantTime_ms + numItems * itemTime_ms, keyedOpMaxRelTimeout_ms);
        return Math.min(constantTime_ms + numItems * itemTime_ms, nonKeyedOpMaxRelTimeout_ms);
    }
    
    /**
     * Create a new OpSizeBasedTimeoutController exactly like this instance, but
     * with the specified maxAttempts.
     * @param maxAttempts maxAttempts for the new instance
     * @return the specified OpSizeBasedTimeoutController
     */
    public OpSizeBasedTimeoutController maxAttempts(int maxAttempts) {
        return new OpSizeBasedTimeoutController(maxAttempts, constantTime_ms, itemTime_ms, /*keyedOpMaxRelTimeout_ms,*/ nonKeyedOpMaxRelTimeout_ms);
    }
    
    /**
     * Create a new OpSizeBasedTimeoutController exactly like this instance, but
     * with the specified constantTimeMillis.
     * @param constantTimeMillis constantTimeMillis for the new instance
     * @return the specified OpSizeBasedTimeoutController
     */
    public OpSizeBasedTimeoutController constantTimeMillis(int constantTimeMillis) {
        return new OpSizeBasedTimeoutController(maxAttempts, constantTimeMillis, itemTime_ms, /*keyedOpMaxRelTimeout_ms,*/ nonKeyedOpMaxRelTimeout_ms);
    }
    
    /**
     * Create a new OpSizeBasedTimeoutController exactly like this instance, but
     * with the specified itemTimeMillis.
     * @param itemTimeMillis itemTimeMillis for the new instance
     * @return the specified OpSizeBasedTimeoutController
     */
    public OpSizeBasedTimeoutController itemTimeMillis(int itemTimeMillis) {
        return new OpSizeBasedTimeoutController(maxAttempts, constantTime_ms, itemTimeMillis, /*keyedOpMaxRelTimeout_ms,*/ nonKeyedOpMaxRelTimeout_ms);
    }
    
    /**
     * Create a new OpSizeBasedTimeoutController exactly like this instance, but
     * with the specified maxRelTimeoutMillis.
     * @param maxRelTimeoutMillis maxRelTimeoutMillis for the new instance
     * @return the specified OpSizeBasedTimeoutController
     */
    //public OpSizeBasedTimeoutController keyedOpMaxRelTimeoutMillis(int keyedOpMaxRelTimeout_ms) {
        //return new OpSizeBasedTimeoutController(maxAttempts, constantTime_ms, itemTime_ms, /*keyedOpMaxRelTimeout_ms,*/ nonKeyedOpMaxRelTimeout_ms);
    //}
    
    /**
     * Create a new OpSizeBasedTimeoutController exactly like this instance, but
     * with the specified maxRelTimeoutMillis.
     * @param maxRelTimeoutMillis maxRelTimeoutMillis for the new instance
     * @return the specified OpSizeBasedTimeoutController
     */
    public OpSizeBasedTimeoutController maxRelTimeoutMillis(int nonKeyedOpMaxRelTimeout_ms) {
        return new OpSizeBasedTimeoutController(maxAttempts, constantTime_ms, itemTime_ms, /*keyedOpMaxRelTimeout_ms,*/ nonKeyedOpMaxRelTimeout_ms);
    }
    
    @Override
    public int hashCode() {
    	return Integer.hashCode(maxAttempts) ^ Integer.hashCode(constantTime_ms) ^ Integer.hashCode(itemTime_ms) ^
    			/*Integer.hashCode(keyedOpMaxRelTimeout_ms) ^*/ Integer.hashCode(nonKeyedOpMaxRelTimeout_ms); 
    }
    
    @Override
    public boolean equals(Object obj) {
    	OpSizeBasedTimeoutController	o;
    	
    	o = (OpSizeBasedTimeoutController)obj;
    	return maxAttempts == o.maxAttempts 
    			&& constantTime_ms == o.constantTime_ms
    			&& itemTime_ms == o.itemTime_ms
    	    	//&& keyedOpMaxRelTimeout_ms == o.keyedOpMaxRelTimeout_ms
    			&& nonKeyedOpMaxRelTimeout_ms == o.nonKeyedOpMaxRelTimeout_ms;
    }
    
    @Override
    public String toString() {
        return ObjectDefParser2.objectToString(this);
    }
    
    /**
     * Parse a definition 
     * @param def object definition 
     * @return a parsed OpSizeBasedTimeoutController instance 
     */
    public static OpSizeBasedTimeoutController parse(String def) {
        return ObjectDefParser2.parse(OpSizeBasedTimeoutController.class, def);
    }
}
