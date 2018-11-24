package com.ms.silverking.cloud.dht.common;

import com.ms.silverking.time.SystemTimeSource;
import com.ms.silverking.time.TimerDrivenTimeSource;
import com.ms.silverking.util.SafeTimer;

public class SystemTimeUtil {
    private static final int	timerDrivenTimeSourceResolutionMS = 5;
    private static final String	timeSourceTimerName = "TimeSourceTimer";
	
	
    public static final SystemTimeSource systemTimeSource 
                            = SystemTimeSource.createWithMillisOrigin(DHTConstants.nanoOriginTimeInMillis);  
    
    public static final TimerDrivenTimeSource	timerDrivenTimeSource = new TimerDrivenTimeSource(new SafeTimer(timeSourceTimerName), timerDrivenTimeSourceResolutionMS);
    
    public static final long systemTimeNanosToEpochMillis(long nanos) {
		//absTimeNanos = (absTimeMillis - nanoOriginTimeMillis) * nanosPerMilli;
    	return (nanos / 1000000) + DHTConstants.nanoOriginTimeInMillis;
    }
}
