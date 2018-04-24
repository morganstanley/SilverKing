package com.ms.silverking.cloud.dht;

import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.client.OpSizeBasedTimeoutController;
import com.ms.silverking.cloud.dht.client.OpTimeoutController;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.common.OptionsHelper;
import com.ms.silverking.text.FieldsRequirement;
import com.ms.silverking.text.ObjectDefParser2;

/**
 * Options for Invalidation operations
 */
public class InvalidationOptions extends PutOptions {
    private static final OpTimeoutController    standardTimeoutController = new OpSizeBasedTimeoutController();
    private static final PutOptions template = OptionsHelper.newInvalidationOptions(standardTimeoutController, PutOptions.defaultVersion, DHTConstants.noSecondaryTargets);

    static {
        ObjectDefParser2.addParser(template, FieldsRequirement.ALLOW_INCOMPLETE);
    }
    
    // FIXME - temp until legacy instances have current defs
	public InvalidationOptions(OpTimeoutController opTimeoutController, Set<SecondaryTarget> secondaryTargets, 
							Compression compression, ChecksumType checksumType, boolean checksumCompressedValues, long version, byte[] userData) {
		super(opTimeoutController, secondaryTargets, compression, checksumType, checksumCompressedValues, version, userData);
	}

    /**
	 * Construct InvalidationOptions from the given arguments. Usage is generally not recommended.
	 * Instead of using this constructor, most applications should obtain an InvalidationOptions
	 * object from a valid source such as the session, the namespace, or the namespace perspective.
     * @param opTimeoutController
     * @param secondaryTargets
     * @param version
     */
	public InvalidationOptions(OpTimeoutController opTimeoutController, Set<SecondaryTarget> secondaryTargets,
								long version) {
		super(opTimeoutController, secondaryTargets, Compression.NONE, ChecksumType.SYSTEM, false, version, null);
	}
	
    /**
     * Return an InvalidationOptions instance like this instance, but with a new OpTimeoutController.
     * @param opTimeoutController the new field value
     * @return the modified InvalidationOptions
     */
	public InvalidationOptions opTimeoutController(OpTimeoutController opTimeoutController) {
		return new InvalidationOptions(opTimeoutController, getSecondaryTargets(), getVersion());
	}
	
    /**
     * Return an InvalidationOptions instance like this instance, but with a new secondaryTargets.
     * @param secondaryTargets the new field value
     * @return the modified InvalidationOptions
     */
	public InvalidationOptions secondaryTargets(Set<SecondaryTarget> secondaryTargets) {
		return new InvalidationOptions(getOpTimeoutController(), secondaryTargets, getVersion());
	}
	
    /**
     * Return an InvalidationOptions instance like this instance, but with a new secondaryTargets.
     * @param secondaryTarget the new field value
     * @return the modified InvalidationOptions
     */
	public InvalidationOptions secondaryTargets(SecondaryTarget secondaryTarget) {
		Preconditions.checkNotNull(secondaryTarget);
		return new InvalidationOptions(getOpTimeoutController(), ImmutableSet.of(secondaryTarget), getVersion());
	}
	
    /**
     * Return an InvalidationOptions instance like this instance, but with a new version.
     * @param version the new field value
     * @return the modified InvalidationOptions
     */
	public InvalidationOptions version(long version) {
		return new InvalidationOptions(getOpTimeoutController(), getSecondaryTargets(), version);
	}
}
