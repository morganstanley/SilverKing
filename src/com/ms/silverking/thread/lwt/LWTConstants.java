package com.ms.silverking.thread.lwt;

import java.util.logging.Level;

import com.ms.silverking.log.Log;

/**
 * Common LWT constants.
 *
 */
public class LWTConstants {
    public static final String  LWTEnvPrefix = "LWT_";
    public static final String  numProcessorsEnvVar = LWTEnvPrefix + "NUM_PROCESSORS";
    public static final int numProcessors;
	
	public static final String propertyBase = LWTConstants.class.getPackage().getName();
	
	private static final int	_defaultMaxDirectCallDepth = 100;
	public static final int	defaultMaxDirectCallDepth;
	static final String	defaultMaxDirectCallDepthProperty = propertyBase +".DefaultMaxDirectCallDepth";
	private static final int	_defaultIdleThreadThreshold = 1;
	public static final int	defaultIdleThreadThreshold;
	static final String	defaultIdleThreadThresholdProperty = propertyBase +".DefaultIdleThreadThreshold";
	
	static final String	enableLoggingProperty = propertyBase +".EnableLogging";
	private static final boolean	_enableLogging = false;
	public static final boolean	enableLogging;
	
    static final String verboseProperty = propertyBase +".Verbose";
    private static final boolean    _verbose = false;
    public static final boolean verbose;
    
	static {
		String	val;
		
        val = System.getenv(numProcessorsEnvVar);
        if (val != null) {
            numProcessors = Integer.parseInt(val);
        } else {
            numProcessors = Runtime.getRuntime().availableProcessors();
        }
        
		val = System.getProperty(defaultMaxDirectCallDepthProperty);
		if (val != null) {
			defaultMaxDirectCallDepth = Integer.parseInt(val);
		} else {
			defaultMaxDirectCallDepth = _defaultMaxDirectCallDepth;
		}
		if (Log.levelMet(Level.INFO)) {
			Log.info(defaultMaxDirectCallDepthProperty +": "+ defaultMaxDirectCallDepth);
		}
		
		val = System.getProperty(defaultIdleThreadThresholdProperty);
		if (val != null) {
			defaultIdleThreadThreshold = Integer.parseInt(val);
		} else {
			defaultIdleThreadThreshold = _defaultIdleThreadThreshold;
		}
		if (Log.levelMet(Level.INFO)) {
			Log.info(defaultIdleThreadThresholdProperty +": "+ defaultIdleThreadThreshold);
		}
		
		val = System.getProperty(enableLoggingProperty);
		if (val != null) {
			enableLogging = Boolean.parseBoolean(val);
		} else {
			enableLogging = _enableLogging;
		}
		if (Log.levelMet(Level.INFO)) {
			Log.info(enableLoggingProperty +": "+ enableLogging);
		}
		
        val = System.getProperty(verboseProperty);
        if (val != null) {
            verbose = Boolean.parseBoolean(val);
        } else {
            verbose = _verbose;
        }
        if (Log.levelMet(Level.INFO)) {
            Log.info(verboseProperty +": "+ verbose);
        }
	}	
}
