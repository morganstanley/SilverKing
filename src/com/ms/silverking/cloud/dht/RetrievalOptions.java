package com.ms.silverking.cloud.dht;

import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.client.OpTimeoutController;
import com.ms.silverking.cloud.dht.client.WaitForTimeoutController;
import com.ms.silverking.cloud.dht.common.OptionsHelper;
import com.ms.silverking.cloud.dht.net.ForwardingMode;
import com.ms.silverking.text.ObjectDefParser2;



/**
 * Options for RetrievalOperations. Specifies what to retrieve, how to respond to non-existent entries, and
 * whether or not to validate value checksums.
 */
public class RetrievalOptions extends OperationOptions {
	private final RetrievalType	       retrievalType;
	private final WaitMode		       waitMode;
	private final VersionConstraint    versionConstraint;
	private final NonExistenceResponse nonExistenceResponse;
	private final boolean              verifyChecksums;
	private final boolean              returnInvalidations;
	private final ForwardingMode       forwardingMode;
	private final boolean              updateSecondariesOnMiss;
	
    // for parsing only
    private static final RetrievalOptions templateOptions = OptionsHelper.newRetrievalOptions(RetrievalType.VALUE, WaitMode.GET);
	
    static {
        ObjectDefParser2.addParser(templateOptions);
        ObjectDefParser2.addSetType(RetrievalOptions.class, "secondaryTargets", SecondaryTarget.class);
    }
    
    /**
     * Construct a fully-specified RetrievalOptions.
     * Usage should be avoided; an instance should be obtained and modified from an enclosing environment.
     * @param opTimeoutController opTimeoutController for the operation
     * @param secondaryTargets constrains queried secondary replicas 
     * to operation solely on the node that receives this operation
     * @param retrievalType type of retrieval
     * @param waitMode whether to perform a WaitFor or a Get
     * @param versionConstraint specify the version
     * @param nonExistenceResponse action to perform for non-existent keys
     * @param verifyChecksums whether or not to verify checksums
     * @param returnInvalidations normally false, true causes invalidated values to be returned.
     * only valid for META_DATA retrievals
     * @param forwardingMode FORWARD is for normal operation. DO_NOT_FORWARD restricts the retrieval
     * to the receiving node
     * @param updateSecondariesOnMiss update secondary replicas when a value is not found at the
     * replica, but is found at the primary 
     */
    public RetrievalOptions(OpTimeoutController opTimeoutController, Set<SecondaryTarget> secondaryTargets, 
                            RetrievalType retrievalType, WaitMode waitMode,
                            VersionConstraint versionConstraint, 
                            NonExistenceResponse nonExistenceResponse, boolean verifyChecksums, 
                            boolean returnInvalidations, ForwardingMode forwardingMode,
                            boolean updateSecondariesOnMiss) {
        super(opTimeoutController, secondaryTargets);
        if (waitMode == WaitMode.WAIT_FOR) {
            if (!(opTimeoutController instanceof WaitForTimeoutController)) {
                throw new IllegalArgumentException(
                      "opTimeoutController must be an a descendant of WaitForTimeoutController for WaitFor operations");
            }
        }
        Preconditions.checkNotNull(retrievalType);
        Preconditions.checkNotNull(waitMode);
        Preconditions.checkNotNull(versionConstraint);
        Preconditions.checkNotNull(nonExistenceResponse);
        this.retrievalType = retrievalType;
        this.waitMode = waitMode;
        this.versionConstraint = versionConstraint;
        this.nonExistenceResponse = nonExistenceResponse;
        this.verifyChecksums = verifyChecksums;
        this.returnInvalidations = returnInvalidations;
        if (returnInvalidations && (retrievalType != RetrievalType.META_DATA && retrievalType != RetrievalType.EXISTENCE)) {
        	throw new IllegalArgumentException("returnInvalidations is incompatible with "+ retrievalType);
        }
        this.forwardingMode = forwardingMode;
        this.updateSecondariesOnMiss = updateSecondariesOnMiss;
    }
    
    /**
     * Return a RetrievalOptions instance like this instance, but with a new opTimeoutController.
     * @param opTimeoutController the new field value
     * @return the modified RetrievalOptions
     */
    public RetrievalOptions opTimeoutController(OpTimeoutController opTimeoutController) {
        return new RetrievalOptions(opTimeoutController, getSecondaryTargets(), 
					                retrievalType, waitMode, versionConstraint, 
					                nonExistenceResponse, verifyChecksums, 
					                returnInvalidations, forwardingMode,
					                updateSecondariesOnMiss);
    }

    /**
     * Return a RetrievalOptions instance like this instance, but with a new secondaryTargets.
     * @param secondaryTargets the new field value
     * @return the modified RetrievalOptions
     */
    public RetrievalOptions secondaryTargets(Set<SecondaryTarget> secondaryTargets) {
        return new RetrievalOptions(getOpTimeoutController(), secondaryTargets, 
					                retrievalType, waitMode, versionConstraint, 
					                nonExistenceResponse, verifyChecksums, 
					                returnInvalidations, forwardingMode,
					                updateSecondariesOnMiss);
    }

    /**
     * Return a RetrievalOptions instance like this instance, but with a new secondaryTargets.
     * @param secondaryTargets the new field value
     * @return the modified RetrievalOptions
     */
    public RetrievalOptions secondaryTargets(SecondaryTarget secondaryTarget) {
        Preconditions.checkNotNull(secondaryTarget);
        return new RetrievalOptions(getOpTimeoutController(), ImmutableSet.of(secondaryTarget), 
					                retrievalType, waitMode, versionConstraint, 
					                nonExistenceResponse, verifyChecksums, 
					                returnInvalidations, forwardingMode,
					                updateSecondariesOnMiss);
    }
    
    /**
     * Return a RetrievalOptions instance like this instance, but with a new retrievalType.
     * @param retrievalType the new field value
     * @return the modified RetrievalOptions
     */
    public RetrievalOptions retrievalType(RetrievalType retrievalType) {
        return new RetrievalOptions(getOpTimeoutController(), getSecondaryTargets(), 
					                retrievalType, waitMode, versionConstraint, 
					                nonExistenceResponse, verifyChecksums, 
					                returnInvalidations, forwardingMode,
					                updateSecondariesOnMiss);
    }
    
    /**
     * Return a RetrievalOptions instance like this instance, but with a new waitMode.
     * @param waitMode the new field value
     * @return the modified RetrievalOptions
     */
	public RetrievalOptions waitMode(WaitMode waitMode) {
        return new RetrievalOptions(getOpTimeoutController(), getSecondaryTargets(), 
					                retrievalType, waitMode, versionConstraint, 
					                nonExistenceResponse, verifyChecksums, 
					                returnInvalidations, forwardingMode,
					                updateSecondariesOnMiss);
	}
	
    /**
     * Return a RetrievalOptions instance like this instance, but with a new versionConstraint.
     * @param versionConstraint the new field value
     * @return the modified RetrievalOptions
     */
    public RetrievalOptions versionConstraint(VersionConstraint versionConstraint) {
        return new RetrievalOptions(getOpTimeoutController(), getSecondaryTargets(), 
					                retrievalType, waitMode, versionConstraint, 
					                nonExistenceResponse, verifyChecksums, 
					                returnInvalidations, forwardingMode,
					                updateSecondariesOnMiss);
    }
    
    /**
     * Return a RetrievalOptions instance like this instance, but with a new nonExistenceResponse.
     * @param nonExistenceResponse the new field value
     * @return the modified RetrievalOptions
     */
    public RetrievalOptions nonExistenceResponse(NonExistenceResponse nonExistenceResponse) {
        return new RetrievalOptions(getOpTimeoutController(), getSecondaryTargets(), 
					                retrievalType, waitMode, versionConstraint, 
					                nonExistenceResponse, verifyChecksums, 
					                returnInvalidations, forwardingMode,
					                updateSecondariesOnMiss);
    }

    /**
     * Return a RetrievalOptions instance like this instance, but with a new verifyChecksums.
     * @param verifyChecksums the new field value
     * @return the modified RetrievalOptions
     */
    public RetrievalOptions verifyChecksums(boolean verifyChecksums) {
        return new RetrievalOptions(getOpTimeoutController(), getSecondaryTargets(), 
					                retrievalType, waitMode, versionConstraint, 
					                nonExistenceResponse, verifyChecksums, 
					                returnInvalidations, forwardingMode,
					                updateSecondariesOnMiss);
    }

    /**
     * Return a RetrievalOptions instance like this instance, but with a new returnInvalidations.
     * @param returnInvalidations the new field value
     * @return the modified RetrievalOptions
     */
    public RetrievalOptions returnInvalidations(boolean returnInvalidations) {
        return new RetrievalOptions(getOpTimeoutController(), getSecondaryTargets(), 
					                retrievalType, waitMode, versionConstraint, 
					                nonExistenceResponse, verifyChecksums, 
					                returnInvalidations, forwardingMode,
					                updateSecondariesOnMiss);
    }
    
    /**
     * Return a RetrievalOptions instance like this instance, but with a new forwardingMode.
     * @param forwardingMode the new field value
     * @return the modified RetrievalOptions
     */
    public RetrievalOptions forwardingMode(ForwardingMode forwardingMode) {
        return new RetrievalOptions(getOpTimeoutController(), getSecondaryTargets(), 
					                retrievalType, waitMode, versionConstraint, 
					                nonExistenceResponse, verifyChecksums, 
					                returnInvalidations, forwardingMode,
					                updateSecondariesOnMiss);
    }

    /**
     * Return a RetrievalOptions instance like this instance, but with a new updateSecondariesOnMiss.
     * @param updateSecondariesOnMiss the new field value
     * @return the modified RetrievalOptions
     */
    public RetrievalOptions updateSecondariesOnMiss(boolean updateSecondariesOnMiss) {
        return new RetrievalOptions(getOpTimeoutController(), getSecondaryTargets(), 
					                retrievalType, waitMode, versionConstraint, 
					                nonExistenceResponse, verifyChecksums, 
					                returnInvalidations, forwardingMode,
					                updateSecondariesOnMiss);
    }
        
	/**
	 * Return retrievalType
	 * @return retrievalType
	 */
	public RetrievalType getRetrievalType() {
		return retrievalType;
	}
	
	/**
	 * Return waitMode
	 * @return waitMode
	 */
	public final WaitMode getWaitMode() {
		return waitMode;
	}
	
	/**
	 * Return versionConstraint
	 * @return versionConstraint
	 */
	public final VersionConstraint getVersionConstraint() {
	    return versionConstraint;
	}
	
	/**
	 * Return nonexistenceResponse
	 * @return nonexistenceResponse
	 */
	public final NonExistenceResponse getNonExistenceResponse() {
	    return nonExistenceResponse;
	}
	
    /**
     * Return verifyChecksums
     * @return verifyChecksums
     */
    public boolean getVerifyChecksums() {
        return verifyChecksums;
    }
    
    /**
     * Return returnInvalidations
     * @return returnInvalidations
     */
    public boolean getReturnInvalidations() {
        return returnInvalidations;
    }
    
    /**
     * Return forwardingMode
     * @return forwardingMode
     */
    public ForwardingMode getForwardingMode() {
        return forwardingMode;
    }

    /**
     * Return updateSecondariesOnMiss
     * @return updateSecondariesOnMiss
     */
    public boolean getUpdateSecondariesOnMiss() {
        return updateSecondariesOnMiss;
    }
    
	@Override
	public int hashCode() {
		return super.hashCode() 
				^ retrievalType.hashCode() 
				^ waitMode.hashCode() 
				^ versionConstraint.hashCode()
				^ nonExistenceResponse.hashCode() 
				^ Boolean.hashCode(verifyChecksums)
				^ Boolean.hashCode(returnInvalidations) 
				^ forwardingMode.hashCode() 
				^ Boolean.hashCode(updateSecondariesOnMiss);
	}
	
	@Override
	public boolean equals(Object other) {
	    if (this == other) {
	        return true;
	    } else {
	        RetrievalOptions   oRetrievalOptions;
	        
	        oRetrievalOptions = (RetrievalOptions)other;
	        return this.retrievalType == oRetrievalOptions.retrievalType
	                && this.waitMode == oRetrievalOptions.waitMode
	                && this.versionConstraint.equals(oRetrievalOptions.versionConstraint)
	                && this.nonExistenceResponse == oRetrievalOptions.nonExistenceResponse
	                && this.verifyChecksums == oRetrievalOptions.verifyChecksums
	                && this.returnInvalidations == oRetrievalOptions.returnInvalidations
	                && this.forwardingMode == oRetrievalOptions.forwardingMode
	                && this.updateSecondariesOnMiss == oRetrievalOptions.updateSecondariesOnMiss
	                && super.equals(other);
	    }
	}
	
	@Override
	public String toString() {
        return ObjectDefParser2.objectToString(this);
	}
	
    /*
     * Parse a RetrievalOptions definition
     * @param def a RetrievalOptions definition  
     * @return a parsed RetrievalOptions instance
     */
    public static RetrievalOptions parse(String def) {
        return ObjectDefParser2.parse(RetrievalOptions.class, def);
    }
}
