package com.ms.silverking.cloud.dht;

import com.google.common.base.Preconditions;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.code.Constraint;
import com.ms.silverking.object.ObjectUtil;
import com.ms.silverking.text.ObjectDefParser2;


/**
 * <p>Options used to configure a namespace. These are fixed at namespace creation time and may not be
 * changed afterwards.</p>
 */
public class NamespaceOptions {
    private final StorageType           storageType;
    private final ConsistencyProtocol   consistencyProtocol;
    private final NamespaceVersionMode  versionMode;
    private final RevisionMode          revisionMode;
    private final PutOptions            defaultPutOptions;
    private final InvalidationOptions   defaultInvalidationOptions;
    private final GetOptions            defaultGetOptions;
    private final WaitOptions           defaultWaitOptions;
    private final int                   secondarySyncIntervalSeconds;
    private final int                   segmentSize;
    private final boolean               allowLinks;
    private final ValueRetentionPolicy	valueRetentionPolicy;
    private final NamespaceServerSideCode	namespaceServerSideCode;
    
    /*
     * To Add:
     * expiration time
     * retention:
     *      time
     *      versions
     *      space
     * max data (total)? topology unit?
     * 
     */
    
    /** minimum allowed segmentSize */
    public static final int                    minSegmentSize = 4 * 1024;
    /** maximum allowed segmentSize */
    public static final int                     maxSegmentSize = 1 * 1024 * 1024 * 1024;
    
    // allowLinks is for SilverRails compatibility only; hide the default here to avoid usage
    static final boolean	defaultAllowLinks = false;
    
    private static final long					defaultInvalidatedRetentionIntervalSeconds = 1 * 60;
    static final ValueRetentionPolicy<InvalidatedRetentionState>	defaultRetentionPolicy = new InvalidatedRetentionPolicy(defaultInvalidatedRetentionIntervalSeconds);
    private static final NamespaceServerSideCode	defaultNamespaceServerSideCode = new NamespaceServerSideCode("", "", "");

    // for parsing only
    static final NamespaceOptions templateOptions = new NamespaceOptions();
    
    static {
        ObjectDefParser2.addParser(templateOptions);
    }

    /**
     * internal use only
     */
    public static void init() {
    }
    
    /**
     * NamespaceOptions constructor. This is for backwards compatibility with SilverRails only. 
     * @param storageType StorageType for this namespace
     * @param consistencyProtocol ConsistencyProtocol for this namespace
     * @param versionMode VersionMode for this namespace
     * @param revisionMode RevisionMode for this namespace
     * @param defaultPutOptions the default PutOptions to use for this namespace
     * @param defaultInvalidationOptions the default InvalidationOptions to use for this namespace
     * @param defaultGetOptions the default GetOptions to use for this namespace
     * @param defaultWaitOptions the default WaitOptions to use for this namespace
     * @param secondarySyncIntervalSeconds interval at which secondary replicas will sync data 
     * @param segmentSize the segment size to use for this namespace
     * @param allowLinks Avoid use. For backwards compatibility to SilverRails only. 
     * @param namespaceServerSideCode TODO
     */
    public NamespaceOptions(StorageType storageType, ConsistencyProtocol consistencyProtocol,
            NamespaceVersionMode versionMode, RevisionMode revisionMode, 
            PutOptions defaultPutOptions, InvalidationOptions defaultInvalidationOptions, 
            GetOptions defaultGetOptions, WaitOptions defaultWaitOptions, 
            int secondarySyncIntervalSeconds, int segmentSize, boolean allowLinks,
            ValueRetentionPolicy valueRetentionPolicy, NamespaceServerSideCode namespaceServerSideCode) {
        Preconditions.checkNotNull(storageType);
        Preconditions.checkNotNull(consistencyProtocol);
        Preconditions.checkNotNull(versionMode);
        Preconditions.checkNotNull(revisionMode);
        Preconditions.checkNotNull(defaultPutOptions);
        Preconditions.checkNotNull(defaultInvalidationOptions);
        checkTimeoutControllerForValidity(defaultGetOptions);
        checkTimeoutControllerForValidity(defaultWaitOptions);
        Preconditions.checkNotNull(valueRetentionPolicy);
        
        this.storageType = storageType;
        this.consistencyProtocol = consistencyProtocol;
        this.versionMode = versionMode;
        this.revisionMode = revisionMode;
        this.defaultPutOptions = defaultPutOptions;
        this.defaultInvalidationOptions = defaultInvalidationOptions;
        this.defaultGetOptions = defaultGetOptions;
        this.defaultWaitOptions = defaultWaitOptions;
        Constraint.checkBounds(minSegmentSize, maxSegmentSize, segmentSize);
        this.secondarySyncIntervalSeconds = secondarySyncIntervalSeconds;
        this.segmentSize = segmentSize;
        this.allowLinks = allowLinks;
        this.valueRetentionPolicy = valueRetentionPolicy;
        this.namespaceServerSideCode = namespaceServerSideCode;
    }
    
    /**
     * For C++ client only. Do not use.
     */
    public NamespaceOptions(StorageType storageType, ConsistencyProtocol consistencyProtocol,
            NamespaceVersionMode versionMode, RevisionMode revisionMode, 
            PutOptions defaultPutOptions, InvalidationOptions defaultInvalidationOptions, 
            GetOptions defaultGetOptions, WaitOptions defaultWaitOptions, 
            int secondarySyncIntervalSeconds, int segmentSize, boolean allowLinks) {
    	this(storageType, consistencyProtocol, versionMode, revisionMode, defaultPutOptions, 
    			defaultInvalidationOptions, defaultGetOptions, defaultWaitOptions, 
    			secondarySyncIntervalSeconds, segmentSize, allowLinks, defaultRetentionPolicy, null);
    }
    
    private NamespaceOptions() {
        this(DHTConstants.defaultStorageType, DHTConstants.defaultConsistencyProtocol, 
                DHTConstants.defaultVersionMode, DHTConstants.defaultRevisionMode, 
                DHTConstants.standardPutOptions, DHTConstants.standardInvalidationOptions,
                DHTConstants.standardGetOptions, DHTConstants.standardWaitOptions,
                DHTConstants.defaultSecondarySyncIntervalSeconds, 
                DHTConstants.defaultSegmentSize, defaultAllowLinks, 
                defaultRetentionPolicy, defaultNamespaceServerSideCode);
    }
    
    /**
     * Return storageType
     * @return storageType
     */
    public StorageType getStorageType() {
        return storageType;
    }
    
    /**
     * Return consistencyProtocol
     * @return consistencyProtocol
     */
    public ConsistencyProtocol getConsistencyProtocol() {
        return consistencyProtocol;
    }
    
    /**
     * Return versionMode
     * @return versionMode
     */
    public NamespaceVersionMode getVersionMode() {
        return versionMode;
    }
    
    /**
     * Return allowRevisions
     * @return allowRevisions
     */
    public RevisionMode getRevisionMode() {
        return revisionMode;
    }
    
    /**
     * Return default PutOptions
     * @return default PutOptions
     */
    public PutOptions getDefaultPutOptions() {
        return defaultPutOptions;
    }
    
    /**
     * Return default InvalidationOptions
     * @return default InvalidationOptions
     */
    public InvalidationOptions getDefaultInvalidationOptions() {
        return defaultInvalidationOptions;
    }
    
    /**
     * Return default GetOptions
     * @return default GetOptions
     */
    public GetOptions getDefaultGetOptions() {
        return defaultGetOptions;
    }
    
    /**
     * Return default WaitOptions
     * @return default WaitOptions
     */
    public WaitOptions getDefaultWaitOptions() {
        return defaultWaitOptions;
    }
    
    /**
     * Return secondarySyncIntervalSeconds
     * @return secondarySyncIntervalSeconds
     */
    public int getSecondarySyncIntervalSeconds() {
        return secondarySyncIntervalSeconds;
    }
    
    /**
     * Return segmentSize
     * @return segmentSize
     */
    public int getSegmentSize() {
        return segmentSize;
    }
    
    /**
     * Avoid use. For backward compatibility with SilverRails only.
     * @return
     */
    public boolean getAllowLinks() {
        return allowLinks;
    }
    
    /**
     * Return valueRetentionPolicy
     * @return valueRetentionPolicy
     */
    public ValueRetentionPolicy getValueRetentionPolicy() {
    	return valueRetentionPolicy;
    }
    
    public NamespaceServerSideCode getNamespaceServerSideCode() {
    	return namespaceServerSideCode;
    }
    
    /**
     * Returns true iff these options specify "write once" semantics: a NamespaceVersionMode of SINGLE_VERSION
     * and a RevisionMode of NO_REVISIONS.
     * @return true iff this options specify "write once" semantics
     */
    public boolean isWriteOnce() {
        return versionMode == NamespaceVersionMode.SINGLE_VERSION && revisionMode == RevisionMode.NO_REVISIONS;
    }
    
    /**
     * Return a copy of this instance that specifies write once semantics: a NamespaceVersionMode of SINGLE_VERSION
     * and a RevisionMode of NO_REVISIONS.
     * @return copy of this instance with a new StorageType
     */
    public NamespaceOptions asWriteOnce() {
        return new NamespaceOptions(storageType, consistencyProtocol, 
                NamespaceVersionMode.SINGLE_VERSION, RevisionMode.NO_REVISIONS, 
                defaultPutOptions, defaultInvalidationOptions, 
                defaultGetOptions, defaultWaitOptions, secondarySyncIntervalSeconds, 
                segmentSize, allowLinks, valueRetentionPolicy, namespaceServerSideCode);
    }
    
    /**
     * Return a copy of this instance with a new StorageType
     * @param storageType storageType for new instance
     * @return copy of this instance with a new StorageType
     */
    public NamespaceOptions storageType(StorageType storageType) {
        return new NamespaceOptions(storageType, consistencyProtocol, versionMode, revisionMode, 
                defaultPutOptions, defaultInvalidationOptions, 
                defaultGetOptions, defaultWaitOptions, secondarySyncIntervalSeconds, 
                segmentSize, allowLinks, valueRetentionPolicy, namespaceServerSideCode);
    }
    
    /**
     * Return a copy of this instance with a new ConsistencyProtocol
     * @param consistencyProtocol ConsistencyProtocol for new instance
     * @return copy of this instance with a new ConsistencyProtocol
     */
    public NamespaceOptions consistencyProtocol(ConsistencyProtocol consistencyProtocol) {
        return new NamespaceOptions(storageType, consistencyProtocol, versionMode, revisionMode, 
                defaultPutOptions, defaultInvalidationOptions, 
                defaultGetOptions, defaultWaitOptions, secondarySyncIntervalSeconds, 
                segmentSize, allowLinks, valueRetentionPolicy, namespaceServerSideCode);
    }
    
    /**
     * Return a copy of this instance with a new NamespaceVersionMode
     * @param versionMode NamespaceVersionMode for new instance
     * @return copy of this instance with a new NamespaceVersionMode
     */
    public NamespaceOptions versionMode(NamespaceVersionMode versionMode) {
        return new NamespaceOptions(storageType, consistencyProtocol, versionMode, revisionMode, 
                defaultPutOptions, defaultInvalidationOptions, 
                defaultGetOptions, defaultWaitOptions, secondarySyncIntervalSeconds, 
                segmentSize, allowLinks, valueRetentionPolicy, namespaceServerSideCode);
    }
    
    /**
     * Return a copy of this instance with a new allowRevisions
     * @param revisionMode allowRevisions for new instance
     * @return copy of this instance with a new NamespaceVersionMode
     */
    public NamespaceOptions revisionMode(RevisionMode revisionMode) {
        return new NamespaceOptions(storageType, consistencyProtocol, versionMode, revisionMode, 
                defaultPutOptions, defaultInvalidationOptions, 
                defaultGetOptions, defaultWaitOptions, secondarySyncIntervalSeconds, 
                segmentSize, allowLinks, valueRetentionPolicy, namespaceServerSideCode);
    }
    
    private void checkTimeoutControllerForValidity(OperationOptions operationOptions) {
        Preconditions.checkNotNull(operationOptions);
        if (!operationOptions.getOpTimeoutController().getClass().getName().startsWith(DHTConstants.systemClassBase)) {
            throw new RuntimeException("Can't use custom OpTimeoutController for NamespaceOptions; "
                                      +"custom only allowed in PutOptions");
        }
    }

    /**
     * Return a copy of this instance with a new default PutOptions
     * @param defaultPutOptions default PutOptions for new instance
     * @return a modified copy of this instance
     */
    public NamespaceOptions defaultPutOptions(PutOptions defaultPutOptions) {
    	if (defaultPutOptions instanceof InvalidationOptions) {
    		throw new IllegalArgumentException("InvalidationOptions not allowed for defaultPutOptions");
    	}
        return new NamespaceOptions(storageType, consistencyProtocol, versionMode, revisionMode, 
                defaultPutOptions, defaultInvalidationOptions, 
                defaultGetOptions, defaultWaitOptions, secondarySyncIntervalSeconds, 
                segmentSize, allowLinks, valueRetentionPolicy, namespaceServerSideCode);
    }

    /**
     * Return a copy of this instance with a new default InvalidationOptions
     * @param defaultInvalidationOptions default InvalidationOptions for new instance
     * @return a modified copy of this instance
     */
    public NamespaceOptions defaultInvalidationOptions(InvalidationOptions defaultInvalidationOptions) {
        return new NamespaceOptions(storageType, consistencyProtocol, versionMode, revisionMode, 
                defaultPutOptions, defaultInvalidationOptions, 
                defaultGetOptions, defaultWaitOptions, secondarySyncIntervalSeconds, 
                segmentSize, allowLinks, valueRetentionPolicy, namespaceServerSideCode);
    }

    /**
     * Return a copy of this instance with a new default GetOptions
     * @param defaultGetOptions default GetOptions for new instance
     * @return a modified copy of this instance
     */
    public NamespaceOptions defaultGetOptions(GetOptions defaultGetOptions) {
        return new NamespaceOptions(storageType, consistencyProtocol, versionMode, revisionMode, 
                defaultPutOptions, defaultInvalidationOptions, 
                defaultGetOptions, defaultWaitOptions, secondarySyncIntervalSeconds, 
                segmentSize, allowLinks, valueRetentionPolicy, namespaceServerSideCode);
    }

    /**
     * Return a copy of this instance with a new default WaitOptions
     * @param defaultWaitOptions default WaitOptions for new instance
     * @return a modified copy of this instance
     */
    public NamespaceOptions defaultWaitOptions(WaitOptions defaultWaitOptions) {
        return new NamespaceOptions(storageType, consistencyProtocol, versionMode, revisionMode, 
                defaultPutOptions, defaultInvalidationOptions, 
                defaultGetOptions, defaultWaitOptions, secondarySyncIntervalSeconds, 
                segmentSize, allowLinks, valueRetentionPolicy, namespaceServerSideCode);
    }

    /**
     * Return a copy of this instance with a new secondary sync interval
     * @param secondarySyncIntervalSeconds secondarySyncIntervalSecondsfor new instance
     * @return copy of this instance with a new secondarySyncIntervalSeconds
     */
    public NamespaceOptions secondarySyncIntervalSeconds(int secondarySyncIntervalSeconds) {
        return new NamespaceOptions(storageType, consistencyProtocol, versionMode, revisionMode, 
                defaultPutOptions, defaultInvalidationOptions, 
                defaultGetOptions, defaultWaitOptions, secondarySyncIntervalSeconds, 
                segmentSize, allowLinks, valueRetentionPolicy, namespaceServerSideCode);
    }
    
    /**
     * Return a copy of this instance with a new segment size
     * @param segmentSize segmentSize for new instance
     * @return copy of this instance with a new segment size
     */
    public NamespaceOptions segmentSize(int segmentSize) {
        return new NamespaceOptions(storageType, consistencyProtocol, versionMode, revisionMode, 
            defaultPutOptions, defaultInvalidationOptions, 
            defaultGetOptions, defaultWaitOptions, secondarySyncIntervalSeconds, 
            segmentSize, allowLinks, valueRetentionPolicy, namespaceServerSideCode);
    }
    
    /**
     * Avoid use. For backwards compatibility with SilverRails only. 
	 * @param allowLinks
	 * @return copy of this instance with new allow links
     */
    public NamespaceOptions allowLinks(boolean allowLinks) {
        return new NamespaceOptions(storageType, consistencyProtocol, versionMode, revisionMode, 
                defaultPutOptions, defaultInvalidationOptions, 
                defaultGetOptions, defaultWaitOptions, secondarySyncIntervalSeconds, 
                segmentSize, allowLinks, valueRetentionPolicy, namespaceServerSideCode);
    }
    
    /**
     * Return a copy of this instance with a new valueRetentionPolicy
     * @param valueRetentionPolicy valueRetentionPolicy for new instance
     * @return copy of this instance with a new valueRetentionPolicy
     */
    public NamespaceOptions valueRetentionPolicy(ValueRetentionPolicy valueRetentionPolicy) {
        return new NamespaceOptions(storageType, consistencyProtocol, versionMode, revisionMode, 
                defaultPutOptions, defaultInvalidationOptions, 
                defaultGetOptions, defaultWaitOptions, secondarySyncIntervalSeconds, 
                segmentSize, allowLinks, valueRetentionPolicy, namespaceServerSideCode);
    }
    
    /**
     * Return a copy of this instance with a new namespaceServerSideCode
     * @param namespaceServerSideCode namespaceServerSideCode for new instance
     * @return copy of this instance with a new namespaceServerSideCode
     */
    public NamespaceOptions namespaceServerSideCode(NamespaceServerSideCode namespaceServerSideCode) {
        return new NamespaceOptions(storageType, consistencyProtocol, versionMode, revisionMode, 
                defaultPutOptions, defaultInvalidationOptions, 
                defaultGetOptions, defaultWaitOptions, secondarySyncIntervalSeconds, 
                segmentSize, allowLinks, valueRetentionPolicy, namespaceServerSideCode);
    }
    
    @Override
    public int hashCode() {
        return storageType.hashCode() 
        		^ consistencyProtocol.hashCode() 
        		^ versionMode.hashCode()
                ^ revisionMode.hashCode() 
                ^ defaultPutOptions.hashCode() 
                ^ defaultInvalidationOptions.hashCode()
                ^ defaultGetOptions.hashCode()
                ^ defaultWaitOptions.hashCode()
                ^ secondarySyncIntervalSeconds 
                ^ segmentSize
                ^ Boolean.hashCode(allowLinks)
                ^ valueRetentionPolicy.hashCode()
                ^ ObjectUtil.hashCode(namespaceServerSideCode);
    }
    
    @Override
    public boolean equals(Object o) {
        NamespaceOptions    other;
        
    	if (this == o) {
    		return true;
    	}
    	
    	if (this.getClass() != o.getClass()) {
    		return false;
    	}
    	        
        other = (NamespaceOptions)o;
        return storageType == other.storageType
                && consistencyProtocol == other.consistencyProtocol
                && versionMode == other.versionMode
                && revisionMode == other.revisionMode
                && defaultPutOptions.equals(other.defaultPutOptions)
                && defaultInvalidationOptions.equals(other.defaultInvalidationOptions)
                && defaultGetOptions.equals(other.defaultGetOptions)
                && defaultWaitOptions.equals(other.defaultWaitOptions)
                && secondarySyncIntervalSeconds == other.secondarySyncIntervalSeconds 
                && segmentSize == other.segmentSize
                && allowLinks == other.allowLinks
                && valueRetentionPolicy.equals(other.valueRetentionPolicy)
                && ObjectUtil.equal(namespaceServerSideCode, other.namespaceServerSideCode);
    }
    
    public void debugEquality(Object o) {
        NamespaceOptions    oNamespaceOptions;
        
        oNamespaceOptions = (NamespaceOptions)o;
        System.out.printf("storageType == oNamespaceOptions.storageType %s\n", storageType == oNamespaceOptions.storageType);
        System.out.printf("consistencyProtocol == oNamespaceOptions.consistencyProtocol %s\n", consistencyProtocol == oNamespaceOptions.consistencyProtocol);
        System.out.printf("versionMode == oNamespaceOptions.versionMode %s\n", versionMode == oNamespaceOptions.versionMode);
        System.out.printf("revisionMode == oNamespaceOptions.revisionMode %s\n", revisionMode == oNamespaceOptions.revisionMode);
        System.out.printf("defaultPutOptions.equals(oNamespaceOptions.defaultPutOptions) %s\n", defaultPutOptions.equals(oNamespaceOptions.defaultPutOptions));
        System.out.printf("defaultInvalidationOptions.equals(oNamespaceOptions.defaultInvalidationOptions) %s\n", defaultInvalidationOptions.equals(oNamespaceOptions.defaultInvalidationOptions));
        System.out.printf("defaultGetOptions.equals(oNamespaceOptions.defaultGetOptions) %s\n", defaultGetOptions.equals(oNamespaceOptions.defaultGetOptions));
        System.out.printf("defaultWaitOptions.equals(oNamespaceOptions.defaultWaitOptions) %s\n", defaultWaitOptions.equals(oNamespaceOptions.defaultWaitOptions));
        System.out.printf("secondarySyncIntervalSeconds == oNamespaceOptions.secondarySyncIntervalSeconds %s\n", secondarySyncIntervalSeconds == oNamespaceOptions.secondarySyncIntervalSeconds); 
        System.out.printf("segmentSize == oNamespaceOptions.segmentSize %s\n", segmentSize == oNamespaceOptions.segmentSize);
        System.out.printf("allowLinks == oNamespaceOptions.allowLinks %s\n", allowLinks == oNamespaceOptions.allowLinks);
        System.out.printf("valueRetentionPolicy.equals(oNamespaceOptions.valueRetentionPolicy); %s\n", valueRetentionPolicy.equals(oNamespaceOptions.valueRetentionPolicy));
        System.out.printf("namespaceServerSideCode.equals(oNamespaceOptions.namespaceServerSideCode); %s\n", namespaceServerSideCode.equals(oNamespaceOptions.namespaceServerSideCode));
    }
    
    @Override
    public String toString() {
        return ObjectDefParser2.objectToString(this);
    }

    /**
     * Parse a NamespaceOptions definition
     * @param def a NamespaceOptions definition in SilverKing ObjectDefParser format 
     * @return a parsed NamespaceOptions instance
     */
    public static NamespaceOptions parse(String def) {
        return ObjectDefParser2.parse(NamespaceOptions.class, def);
    }
}
