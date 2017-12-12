package com.ms.silverking.cloud.dht.client;

import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;

/**
 * Controls timeout and retry behavior for operations. AsyncOperation is used here since
 * all operations are asynchronous internally. 
 */
@OmitGeneration
public interface OpTimeoutController {
    /**
     * Return the maximum number of times that this operation should be attempted.
     * @param op the relevant operation 
     * @return the maximum number of times that this operation should be attempted
     */
    public int getMaxAttempts(AsyncOperation op);
    /**
     * Return the relative timeout in milliseconds for the given attempt.
     * @param op the relevant operation
     * @param attemptIndex a zero-based attempt index. Ranges from 0 to the maximum number of attempts - 1.
     * @return the relative timeout in milliseconds for the given attempt
     */
    public int getRelativeTimeoutMillisForAttempt(AsyncOperation op, int attemptIndex);
    /**
     * Return the maximum relative timeout for the given operation. Once this timeout is triggered, no further
     * attempts of this operation will be made irrespective of the individual attempt timeout or the 
     * maximum number of attempts.
     * @param op the relevant operation
     * @return the maximum relative timeout for the given operation
     */
    public int getMaxRelativeTimeoutMillis(AsyncOperation op);
    
    public static final int	min_maxAttempts = 1;
    public static final int minInitialTimeout_ms = 5;
}
