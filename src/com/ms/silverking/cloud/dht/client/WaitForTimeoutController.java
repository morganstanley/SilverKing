package com.ms.silverking.cloud.dht.client;

import java.util.concurrent.TimeUnit;

import com.ms.silverking.cloud.dht.WaitOptions;
import com.ms.silverking.text.ObjectDefParser2;

/**
 * Parent class of all OpTimeoutControllers
 * for WaitFor operations. For these operations, the only parameter that
 * may be specified is the internal retry interval. All other
 * parameters are either implicitly or explicitly specified in
 * the WaitOptions for the operation.
 */
public class WaitForTimeoutController implements OpTimeoutController {
    private final int   internalRetryIntervalSeconds;
    
    static final int    defaultInternalRetryIntervalSeconds = 20;
    
    static final WaitForTimeoutController    template = new WaitForTimeoutController();

    static {
        ObjectDefParser2.addParser(template);
    }    
    
    public WaitForTimeoutController(int internalRetryIntervalSeconds) {
        this.internalRetryIntervalSeconds = internalRetryIntervalSeconds;
    }
    
    public WaitForTimeoutController() {
        this(defaultInternalRetryIntervalSeconds);
    }
    
    @Override
    public int getMaxAttempts(AsyncOperation op) {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getRelativeTimeoutMillisForAttempt(AsyncOperation op,
            int attemptIndex) {
        return internalRetryIntervalSeconds;
    }

    @Override
    public final int getMaxRelativeTimeoutMillis(AsyncOperation op) {
        AsyncRetrieval  asyncRetrieval;
        WaitOptions     waitOptions;
        
        asyncRetrieval = (AsyncRetrieval)op;
        waitOptions = (WaitOptions)asyncRetrieval.getRetrievalOptions();
        if (waitOptions.getTimeoutSeconds() == Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        } else {
            return (int)TimeUnit.MILLISECONDS.convert(waitOptions.getTimeoutSeconds(), TimeUnit.SECONDS);
        }
    }
    
    @Override
    public int hashCode() {
    	return Integer.hashCode(internalRetryIntervalSeconds);
    }
    
    @Override
    public boolean equals(Object o) {
    	if (this == o) {
    		return true;
    	}
    	
    	if (this.getClass() != o.getClass()) {
    		return false;
    	}

    	WaitForTimeoutController other;
    	other = (WaitForTimeoutController)o;
    	return internalRetryIntervalSeconds == other.internalRetryIntervalSeconds;
    }    
    
    @Override
    public String toString() {
        return ObjectDefParser2.objectToString(this);
    }
    
    /**
     * Parse a definition 
     * @param def object definition 
     * @return a parsed instance 
     */
    public static WaitForTimeoutController parse(String def) {
        return ObjectDefParser2.parse(WaitForTimeoutController.class, def);
    }
}
