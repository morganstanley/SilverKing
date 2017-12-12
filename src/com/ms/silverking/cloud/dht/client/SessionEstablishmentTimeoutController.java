package com.ms.silverking.cloud.dht.client;

import com.ms.silverking.cloud.dht.SessionOptions;
import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;

/**
 * Controls timeout and retry behavior for session establishment. 
 */
@OmitGeneration
public interface SessionEstablishmentTimeoutController {
    /**
     * Return the maximum number of times that this session establishment should be attempted.
     * @param options the options of the session being established 
     * @return the maximum number of times that this operation should be attempted
     */
    public int getMaxAttempts(SessionOptions options);
    /**
     * Return the relative timeout in milliseconds for the given attempt.
     * @param options the options of the session being established 
     * @param attemptIndex a zero-based attempt index. Ranges from 0 to the maximum number of attempts - 1.
     * @return the relative timeout in milliseconds for the given attempt
     */
    public int getRelativeTimeoutMillisForAttempt(SessionOptions options, int attemptIndex);
    /**
     * Return the maximum relative timeout for the given session establishment. 
     * Once this timeout is triggered, no further attempts will be made irrespective 
     * of the individual attempt timeout or the maximum number of attempts.
     * @param options the options of the session being established 
     * @return the maximum relative timeout for the given operation
     */
    public int getMaxRelativeTimeoutMillis(SessionOptions options);
}
