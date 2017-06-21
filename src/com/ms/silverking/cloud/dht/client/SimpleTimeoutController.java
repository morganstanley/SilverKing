package com.ms.silverking.cloud.dht.client;

import com.ms.silverking.text.ObjectDefParser2;

/**
 * OpTimeoutController implementation that supports a specified maximum number of attempts
 * as well as a single relative timeout for the operation. 
 */
public class SimpleTimeoutController implements OpTimeoutController {
    private final int   maxAttempts;
    private final int   maxRelativeTimeoutMillis;
    
    static final int    defaultMaxAttempts = 5;
    static final int    defaultMaxRelativeTimeoutMillis = 2 * 60 * 1000;
    
    static final SimpleTimeoutController    template = new SimpleTimeoutController(defaultMaxAttempts, 
                                                                                      defaultMaxRelativeTimeoutMillis);
    
    static {
        ObjectDefParser2.addParser(template);
    }    
    
    /**
     * Construct a SingleTimeout instance with the specified number of attempts and the specified relative timeout.
     * @param maxAttempts maximum number of attempts
     * @param maxRelativeTimeoutMillis relative timeout in milliseconds
     */
    public SimpleTimeoutController(int maxAttempts, int maxRelativeTimeoutMillis) {
    	Util.checkAttempts(maxAttempts);
        this.maxAttempts = maxAttempts;
        this.maxRelativeTimeoutMillis = maxRelativeTimeoutMillis;
    }
    
    /**
     * Create a SimpleTimeoutController like this instance, but with a new maxAttempts.
     * @return a SimpleTimeoutController like this instance, but with a new maxAttempts
     */
    public SimpleTimeoutController maxAttempts(int maxAttempts) {
        return new SimpleTimeoutController(maxAttempts, maxRelativeTimeoutMillis);
    }

    /**
     * Create a SimpleTimeoutController like this instance, but with a new maxRelativeTimeoutMillis.
     * @return a SimpleTimeoutController like this instance, but with a new maxRelativeTimeoutMillis
     */
    public SimpleTimeoutController maxRelativeTimeoutMillis(int maxRelativeTimeoutMillis) {
        return new SimpleTimeoutController(maxAttempts, maxRelativeTimeoutMillis);
    }
    
    @Override
    public int getMaxAttempts(AsyncOperation op) {
        return maxAttempts;
    }

    @Override
    public int getRelativeTimeoutMillisForAttempt(AsyncOperation op, int attemptIndex) {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getMaxRelativeTimeoutMillis(AsyncOperation op) {
        return maxRelativeTimeoutMillis;
    }
     
    @Override
    public int hashCode() {
    	return maxAttempts ^ maxRelativeTimeoutMillis;
    }
    
    @Override
    public boolean equals(Object o) {
    	SimpleTimeoutController	other;
    	
    	if (this == o) {
    		return true;
    	}
    	
    	if (this.getClass() != o.getClass()) {
    		return false;
    	}
    	
    	other = (SimpleTimeoutController)o;
    	return maxAttempts == other.maxAttempts 
    			&& maxRelativeTimeoutMillis == other.maxRelativeTimeoutMillis;
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
    public static SimpleTimeoutController parse(String def) {
        return ObjectDefParser2.parse(SimpleTimeoutController.class, def);
    }
}
