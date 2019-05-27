package com.ms.silverking.net.async;


/**
 * Globals for this package.
 */
public class AsyncGlobals {
	public static final boolean	debug = false;
	
	private static boolean verbositySet;
	static boolean  verbose = true;
	
	public static void setVerbose(boolean _verbose) {
	    if (!verbositySet) {
	        verbose = _verbose || debug;
	        verbositySet = true;
	    }
	}
}
