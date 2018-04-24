package com.ms.silverking.cloud.dht;

import java.util.Arrays;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.client.OpSizeBasedTimeoutController;
import com.ms.silverking.cloud.dht.client.OpTimeoutController;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.text.FieldsRequirement;
import com.ms.silverking.text.ObjectDefParser2;

/**
 * Options for Put operations. 
 */
public class PutOptions extends OperationOptions {
    private final Compression       compression;
    private final ChecksumType      checksumType;
    private final boolean           checksumCompressedValues;
    private final long              version;
    private final byte[]            userData;
    
    /** Maximum length of user data */
    public static final int    maxUserDataLength = 255;
    
    /** Use the default version of the NamespacePerspective */
    public static final long   defaultVersion = DHTConstants.unspecifiedVersion;

    // begin temp replicated from DHTConstants
    // FUTURE - A limitation in the parser seems to require replication until we have a proper fix
    private static final OpTimeoutController    standardTimeoutController = new OpSizeBasedTimeoutController();
    private static final PutOptions template = new PutOptions(
								            standardTimeoutController, 
								            DHTConstants.noSecondaryTargets, Compression.LZ4, 
								            ChecksumType.MURMUR3_32, 
								            false, 
								            PutOptions.defaultVersion, null);
    // end temp replicated from DHTConstants
    
    static {
        ObjectDefParser2.addParser(template, FieldsRequirement.ALLOW_INCOMPLETE);
        ObjectDefParser2.addSetType(PutOptions.class, "secondaryTargets", SecondaryTarget.class);
    }
	
	/**
	 * Construct PutOptions from the given arguments. Usage is generally not recommended.
	 * Instead of using this constructor, applications should obtain an instance
	 * from a valid source such as the Session, the Namespace, or the NamespacePerspective.
	 * @param opTimeoutController 
	 * @param secondaryTargets
	 * @param compression type of compression to use
	 * @param checksumType checksum to use for value
	 * @param checksumCompressedValues controls whether or not compressed values are checksummed
	 * @param version version to use for a Put operation. Using defaultVersion will allow the version mode
	 * to set this automatically.
	 * @param userData out of band data to store with value. May not exceed maxUserDataLength. 
	 */
	public PutOptions(OpTimeoutController opTimeoutController, Set<SecondaryTarget> secondaryTargets,
					Compression compression, ChecksumType checksumType, boolean checksumCompressedValues, 
					long version, byte[] userData) {
	    super(opTimeoutController, secondaryTargets);
        Preconditions.checkNotNull(compression);
        Preconditions.checkNotNull(checksumType);
		this.compression = compression;
		if (version < 0) {
		    throw new IllegalArgumentException("version can't be < 0: "+ version);
		}
		this.version = version;
		this.userData = userData;
        this.checksumType = checksumType;
        this.checksumCompressedValues = checksumCompressedValues;
	}
	
    /**
     * Return a PutOptions instance like this instance, but with a new OpTimeoutController.
     * @param opTimeoutController OpTimeoutController to use
     * @return the modified PutOptions instance
     */
    public PutOptions opTimeoutController(OpTimeoutController opTimeoutController) {
        return new PutOptions(opTimeoutController, getSecondaryTargets(), compression, checksumType, checksumCompressedValues, version, userData);
    }
    
    /**
     * Return an InvalidationOptions instance like this instance, but with a new secondaryTargets.
     * @param secondaryTargets the new field value
     * @return the modified InvalidationOptions
     */
	public PutOptions secondaryTargets(Set<SecondaryTarget> secondaryTargets) {
		return new PutOptions(getOpTimeoutController(), secondaryTargets, getCompression(), 
										getChecksumType(), getChecksumCompressedValues(), getVersion(), getUserData());
	}
	
    /**
     * Return an InvalidationOptions instance like this instance, but with a new secondaryTargets.
     * @param secondaryTarget the new field value
     * @return the modified InvalidationOptions
     */
	public PutOptions secondaryTargets(SecondaryTarget secondaryTarget) {
        Preconditions.checkNotNull(secondaryTarget);
		return new PutOptions(getOpTimeoutController(), ImmutableSet.of(secondaryTarget), getCompression(), 
										getChecksumType(), getChecksumCompressedValues(), getVersion(), getUserData());
	}
    
    /**
     * Return a PutOptions instance like this instance, but with a new compression.
     * @param compression type of compression to use
     * @return the modified PutOptions instance
     */
    public PutOptions compression(Compression compression) {
        return new PutOptions(getOpTimeoutController(), getSecondaryTargets(), compression, checksumType, checksumCompressedValues, version, userData);
    }
    
    /**
     * Return a PutOptions instance like this instance, but with a new checksumType.
     * @param checksumType checksum to use for value
     * @return the modified PutOptions instance
     */
    public PutOptions checksumType(ChecksumType checksumType) {
        return new PutOptions(getOpTimeoutController(), getSecondaryTargets(), compression, checksumType, checksumCompressedValues, version, userData);
    }
    
    /**
     * Return a PutOptions instance like this instance, but with a new checksumCompressedValues.
     * @param checksumCompressedValues checksumCompressedValues to use for value
     * @return the modified PutOptions instance
     */
    public PutOptions checksumCompressedValues(boolean checksumCompressedValues) {
        return new PutOptions(getOpTimeoutController(), getSecondaryTargets(), compression, checksumType, checksumCompressedValues, version, userData);
    }
    
    /**
     * Return a PutOptions instance like this instance, but with a new version.
     * @param version new version to use
     * @return a PutOptions instance like this instance, but with a new version.
     */
    public PutOptions version(long version) {
        return new PutOptions(getOpTimeoutController(), getSecondaryTargets(), compression, checksumType, checksumCompressedValues, 
        		version, userData);
    }
    
    /**
     * Return a PutOptions instance like this instance, but with new userData.
     * @param userData new user data to use
     * @return a PutOptions instance like this instance, but with new userData.
     */
    public PutOptions userData(byte[] userData) {
        return new PutOptions(getOpTimeoutController(), getSecondaryTargets(), compression, checksumType, 
                              checksumCompressedValues, version, userData);
    }
    
	/**
	 * Return compression
	 * @return compression
	 */
	public Compression getCompression() {
		return compression;
	}

    /**
     * Return checksumType
     * @return checksumType
     */
    public ChecksumType getChecksumType() {
        return checksumType;
    }
    
    /**
     * Return checksumCompressedValues
     * @return checksumCompressedValues
     */
    public boolean getChecksumCompressedValues() {
        return checksumCompressedValues;
    }
    
	/**
	 * Return version
	 * @return version
	 */
    public long getVersion() {
        return version;
    }
    
    /**
     * Return userData
     * @return userData
     */
    public byte[] getUserData() {
        return userData;
    }
    
    @Override
    public int hashCode() {
        return super.hashCode() 
        		^ compression.hashCode() 
        		^ checksumType.hashCode() 
        		^ Boolean.hashCode(checksumCompressedValues) 
        		^ Long.hashCode(version)
        		^ Arrays.hashCode(userData);
    }
    
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else {
            PutOptions  oPutOptions;
            
            oPutOptions = (PutOptions)other;
            if (!super.equals(other)) {
            	return false;
            }
            if (this.userData != null) {
                if (oPutOptions.userData != null) {
                    if (!Arrays.equals(this.userData, oPutOptions.userData)) {
                        return false;
                    }
                } else {
                    return false;
                }
            } else {
                if (oPutOptions.userData != null) {
                    return false;
                }
            }
            return  this.compression == oPutOptions.compression
                    && this.checksumType == oPutOptions.checksumType
                    && this.checksumCompressedValues == oPutOptions.checksumCompressedValues
                    && this.version == oPutOptions.version;
        }
    }
	
	@Override
	public String toString() {
	    return ObjectDefParser2.objectToString(this);
	}
	
	/**
	 * Parse a definition 
	 * @param def object definition in ObjectDefParser format
	 * @return a parsed PutOptions instance 
	 */
    public static PutOptions parse(String def) {
        return ObjectDefParser2.parse(PutOptions.class, def);
    }
}
