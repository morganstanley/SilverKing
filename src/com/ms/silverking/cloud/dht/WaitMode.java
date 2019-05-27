package com.ms.silverking.cloud.dht;

/**
 * Specifies how retrievals should respond to non-existent entries. 
 */
public enum WaitMode {
	/** Return the requested data, or lack thereof, as soon as possible without waiting for data to be stored */
	GET, 
	/** Wait for any non-existing entries to be stored (subject to WaitOptions.) */
	WAIT_FOR
}
