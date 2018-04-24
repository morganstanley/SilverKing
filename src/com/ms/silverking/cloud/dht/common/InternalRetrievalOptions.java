package com.ms.silverking.cloud.dht.common;

import com.ms.silverking.cloud.dht.ConsistencyProtocol;
import com.ms.silverking.cloud.dht.NonExistenceResponse;
import com.ms.silverking.cloud.dht.RetrievalOptions;
import com.ms.silverking.cloud.dht.RetrievalType;
import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.WaitMode;
import com.ms.silverking.cloud.dht.serverside.SSRetrievalOptions;

/**
 * Adds internally useful information to RetrievalOptions that should not be exposed to
 * end users. Also removes RetrievalOptions that only apply in the client.
 */
public class InternalRetrievalOptions implements SSRetrievalOptions {
    private final RetrievalOptions  retrievalOptions;
    private final boolean           verifyIntegrity;
    private final ConsistencyProtocol	cpSSToVerify; // ConsistencyProtocol to verify storage state against
    												  // non-null value implies that state should be verified
    
    public InternalRetrievalOptions(RetrievalOptions retrievalOptions, boolean verifyIntegrity, ConsistencyProtocol	cpSSToVerify) {
        this.retrievalOptions = retrievalOptions;
        this.verifyIntegrity = verifyIntegrity;
        this.cpSSToVerify = cpSSToVerify;
    }
    
    public InternalRetrievalOptions(RetrievalOptions retrievalOptions, boolean verifyIntegrity) {
    	this(retrievalOptions, verifyIntegrity, null);
    }
    
    public InternalRetrievalOptions(RetrievalOptions retrievalOptions) {
        this(retrievalOptions, false, null);
    }
    
	public static InternalRetrievalOptions fromSSRetrievalOptions(SSRetrievalOptions options) {
		if (options instanceof InternalRetrievalOptions) {
			return (InternalRetrievalOptions)options;
		} else {
			RetrievalOptions	retrievalOptions;
			
			retrievalOptions = new RetrievalOptions(null, null, options.getRetrievalType(), WaitMode.GET, options.getVersionConstraint(), null, options.getVerifyIntegrity(), options.getReturnInvalidations(), null, false);
			return new InternalRetrievalOptions(retrievalOptions, options.getVerifyIntegrity());
		}
	}
    
    public InternalRetrievalOptions retrievalOptions(RetrievalOptions retrievalOptions) {
    	return new InternalRetrievalOptions(retrievalOptions, verifyIntegrity, cpSSToVerify); 
    }
    
    public InternalRetrievalOptions retrievalType(RetrievalType retrievalType) {
		return retrievalOptions(retrievalOptions.retrievalType(retrievalType));
    }
    
    public RetrievalOptions getRetrievalOptions() {
        return retrievalOptions;
    }

    public boolean getVerifyIntegrity() {
        return verifyIntegrity;
    }
    
    public ConsistencyProtocol getCPSSToVerify() {
    	return cpSSToVerify;
    }
    
    public boolean getVerifyStorageState() {
        return cpSSToVerify != null;
    }
    
	@Override
	public boolean getReturnInvalidations() {
		return retrievalOptions.getReturnInvalidations();
	}
    
    /**
     * 
     * @return
     */
    public RetrievalType getRetrievalType() {
        return retrievalOptions.getRetrievalType();
    }
    
    /**
     * waitMode getter
     * @return waidMode
     */
    public final WaitMode getWaitMode() {
        return retrievalOptions.getWaitMode();
    }
    
    /**
     * versionConstraint getter
     * @return
     */
    public final VersionConstraint getVersionConstraint() {
        return retrievalOptions.getVersionConstraint();
    }
    
    /**
     * nonexistenceResponse getter
     * @return
     */
    public final NonExistenceResponse getNonExistenceResponse() {
        return retrievalOptions.getNonExistenceResponse();
    }
    
    /**
     * Return copy of this object with modified VersionConstraint
     * @param vc
     * @return
     */
    public InternalRetrievalOptions versionConstraint(VersionConstraint vc) {
        return new InternalRetrievalOptions(retrievalOptions.versionConstraint(vc), verifyIntegrity, cpSSToVerify);
    }
    
    /**
     * Return copy of this object with modified WaitMode
     * @param vc
     * @return
     */
    public InternalRetrievalOptions waitMode(WaitMode waitMode) {
        return new InternalRetrievalOptions(retrievalOptions.waitMode(waitMode), verifyIntegrity, cpSSToVerify);
    }
    
    /**
     * Return copy of this object with modified verifyStorageState
     * @param vc
     * @return
     */
    public InternalRetrievalOptions cpSSToVerify(ConsistencyProtocol cpSSToVerify) {
        return new InternalRetrievalOptions(retrievalOptions, verifyIntegrity, cpSSToVerify);
    }
    
    @Override
    public int hashCode() {
        throw new RuntimeException("not implemented");
    }
    
    @Override
    public boolean equals(Object other) {
        throw new RuntimeException("not implemented");
    }
    
    @Override
    public String toString() {
        StringBuilder   sb;
        
        sb = new StringBuilder();
        sb.append(retrievalOptions);
        sb.append(':');
        sb.append(verifyIntegrity);
        return sb.toString();
    }
}
