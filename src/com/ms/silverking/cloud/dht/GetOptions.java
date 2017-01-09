package com.ms.silverking.cloud.dht;

import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.client.OpTimeoutController;
import com.ms.silverking.cloud.dht.client.SimpleTimeoutController;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.common.OptionsHelper;
import com.ms.silverking.cloud.dht.net.ForwardingMode;
import com.ms.silverking.text.ObjectDefParser2;


/**
 * Options for Get operations. (RetrievalOptions with WaitMode fixed at GET.) 
 */
public final class GetOptions extends RetrievalOptions {
    private static final Set<String>    exclusionFields = ImmutableSet.of("waitMode", "forwardingMode");
    
    private static final GetOptions    template = OptionsHelper.newGetOptions(
            DHTConstants.standardTimeoutController, RetrievalType.VALUE, VersionConstraint.defaultConstraint);
    
    static {
        ObjectDefParser2.addParserWithExclusions(template, exclusionFields);
    }
    
    /**
     * Construct a fully-specified GetOptions.
     * Usage should be avoided; an instance should be obtained and modified from an enclosing environment.
     * @param opTimeoutController opTimeoutController for the operation
     * @param secondaryTargets constrains queried secondary replicas 
     * to operation solely on the node that receives this operation
     * @param retrievalType type of retrieval
     * @param versionConstraint specify the version
     * @param nonExistenceResponse action to perform for non-existent keys
     * @param verifyChecksums whether or not to verify checksums
     * @param returnInvalidations normally false, true causes invalidated values to be returned.
     * only valid for META_DATA retrievals
     * @param updateSecondariesOnMiss update secondary replicas when a value is not found at the
     * replica, but is found at the primary
     */
    public GetOptions(OpTimeoutController opTimeoutController, Set<SecondaryTarget> secondaryTargets, 
            RetrievalType retrievalType, VersionConstraint versionConstraint, 
            NonExistenceResponse nonExistenceResponse, boolean verifyChecksums, 
            boolean returnInvalidations, boolean updateSecondariesOnMiss) {
        super(opTimeoutController, secondaryTargets, retrievalType, WaitMode.GET, versionConstraint, 
        		nonExistenceResponse, verifyChecksums, returnInvalidations, ForwardingMode.FORWARD, false);
    }    
    
    /**
     * Return a GetOptions instance like this instance, but with a new OpTimeoutController.
     * @param opTimeoutController the new field value
     * @return the modified GetOptions
     */
    public GetOptions opTimeoutController(OpTimeoutController opTimeoutController) {
        Preconditions.checkNotNull(opTimeoutController);
        return new GetOptions(opTimeoutController, getSecondaryTargets(), 
                getRetrievalType(), getVersionConstraint(), 
                getNonExistenceResponse(), getVerifyChecksums(), 
                getReturnInvalidations(), getUpdateSecondariesOnMiss());
    }
    
    /**
     * Return a GetOptions instance like this instance, but with a new SecondaryTargets.
     * @param secondaryTargets the new field value
     * @return the modified GetOptions
     */
    public GetOptions secondaryTargets(Set<SecondaryTarget> secondaryTargets) {
        Preconditions.checkNotNull(secondaryTargets);
        return new GetOptions(getOpTimeoutController(), secondaryTargets, 
                getRetrievalType(), getVersionConstraint(), 
                getNonExistenceResponse(), getVerifyChecksums(), 
                getReturnInvalidations(), getUpdateSecondariesOnMiss());
    }
    
    /**
     * Return a GetOptions instance like this instance, but with a new SecondaryTargets.
     * @param secondaryTargets the new field value
     * @return the modified GetOptions
     */
    public GetOptions secondaryTargets(SecondaryTarget secondaryTarget) {
        Preconditions.checkNotNull(secondaryTarget);
        return new GetOptions(getOpTimeoutController(), ImmutableSet.of(secondaryTarget), 
                getRetrievalType(), getVersionConstraint(), 
                getNonExistenceResponse(), getVerifyChecksums(), 
                getReturnInvalidations(), getUpdateSecondariesOnMiss());
    }
    
    /**
     * Return a GetOptions instance like this instance, but with a new RetrievalType.
     * @param retrievalType the new field value
     * @return the modified GetOptions
     */
    public GetOptions retrievalType(RetrievalType retrievalType) {
        Preconditions.checkNotNull(retrievalType);
        return new GetOptions(getOpTimeoutController(), getSecondaryTargets(), 
                retrievalType, getVersionConstraint(), 
                getNonExistenceResponse(), getVerifyChecksums(), 
                getReturnInvalidations(), getUpdateSecondariesOnMiss());
    }
    
    /**
     * Return a GetOptions instance like this instance, but with a new VersionConstraint.
     * @param versionConstraint the new field value
     * @return the modified GetOptions
     */
    public GetOptions versionConstraint(VersionConstraint versionConstraint) {
        Preconditions.checkNotNull(versionConstraint);
        return new GetOptions(getOpTimeoutController(), getSecondaryTargets(), 
                getRetrievalType(), versionConstraint, 
                getNonExistenceResponse(), getVerifyChecksums(), 
                getReturnInvalidations(), getUpdateSecondariesOnMiss());
    }
    
    /**
     * Return a GetOptions instance like this instance, but with a new NonExistenceResponse.
     * @param nonExistenceResponse the new field value
     * @return the modified GetOptions
     */
    public GetOptions nonExistenceResponse(NonExistenceResponse nonExistenceResponse) {
        Preconditions.checkNotNull(nonExistenceResponse);
        return new GetOptions(getOpTimeoutController(), getSecondaryTargets(), 
                getRetrievalType(), getVersionConstraint(), 
                nonExistenceResponse, getVerifyChecksums(), 
                getReturnInvalidations(), getUpdateSecondariesOnMiss());
    }
    
    /**
     * Return a GetOptions instance like this instance, but with a new verifyChecksums.
     * @param verifyChecksums the new field value
     * @return the modified GetOptions
     */
    public GetOptions verifyChecksums(boolean verifyChecksums) {
        return new GetOptions(getOpTimeoutController(), getSecondaryTargets(), 
                getRetrievalType(), getVersionConstraint(), 
                getNonExistenceResponse(), verifyChecksums, 
                getReturnInvalidations(), getUpdateSecondariesOnMiss());
    }
    
    /**
     * Return a GetOptions instance like this instance, but with a new returnInvalidations.
     * @param returnInvalidations the new field value
     * @return the modified GetOptions
     */
    public GetOptions returnInvalidations(boolean returnInvalidations) {
        return new GetOptions(getOpTimeoutController(), getSecondaryTargets(), 
                getRetrievalType(), getVersionConstraint(), 
                getNonExistenceResponse(), getVerifyChecksums(), 
                returnInvalidations, getUpdateSecondariesOnMiss());
    }
    
    /**
     * Return a GetOptions instance like this instance, but with a new updateSecondariesOnMiss.
     * @param updateSecondariesOnMiss the new field value
     * @return the modified GetOptions
     */
    public GetOptions updateSecondariesOnMiss(boolean updateSecondariesOnMiss) {
        return new GetOptions(getOpTimeoutController(), getSecondaryTargets(), 
                getRetrievalType(), getVersionConstraint(), 
                getNonExistenceResponse(), getVerifyChecksums(), 
                getReturnInvalidations(), updateSecondariesOnMiss);
    }
    
    @Override
    public String toString() {
        return ObjectDefParser2.objectToString(this);
    }
    
    /**
     * Parse a definition 
     * @param def object definition 
     * @return a parsed GetOptions instance 
     */
    public static GetOptions parse(String def) {
        return ObjectDefParser2.parse(GetOptions.class, def);
    }
}
