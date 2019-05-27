package com.ms.silverking.cloud.dht;

/**
 * Options for Delete operations.
 */
public class DeletionOptions {
    private final long              version;

    /**
     * Create a new instance
     * @param version version that deletion takes place 
     */
    public DeletionOptions(long version) {
    	this.version = version;
    }
    
    public static DeletionOptions version(long version) {
    	return new DeletionOptions(version);
    }
    
    public long getVersion() {
    	return version;
    }
}
