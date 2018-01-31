package com.ms.silverking.cloud.dht;

import static com.ms.silverking.cloud.dht.NamespaceOptions.*;
import static com.ms.silverking.cloud.dht.TestUtil.*;
import static com.ms.silverking.cloud.dht.common.DHTConstants.*;
import static com.ms.silverking.testing.AssertFunction.*;
import static com.ms.silverking.testing.Util.*;
import static org.junit.Assert.*;

import org.junit.Test;

import com.ms.silverking.cloud.dht.TimeAndVersionRetentionPolicy.Mode;
import com.ms.silverking.code.ConstraintViolationException;
import com.ms.silverking.testing.Util.ExceptionChecker;

public class NamespaceOptionsTest {

	// *Copy: copies the values of nspOptions (copies the object values, rather than re-using the same object from defaultNspOptions, important for comparing reference vs value, i.e. hashCode and equals)
	// *Diff: copies the values of nspOptionsCopy and changes at least one value - this is so that it's different
	private static final StorageType stCopy            = StorageType.FILE;
	private static final StorageType stDiff            = StorageType.FILE_SYNC;

	private static final ConsistencyProtocol cpCopy    = ConsistencyProtocol.TWO_PHASE_COMMIT;
	private static final ConsistencyProtocol cpDiff    = ConsistencyProtocol.LOOSE;
	
	private static final NamespaceVersionMode nsvmCopy = NamespaceVersionMode.SINGLE_VERSION;
	private static final NamespaceVersionMode nsvmDiff = NamespaceVersionMode.SYSTEM_TIME_MILLIS;
	
	private static final RevisionMode rmCopy           = RevisionMode.NO_REVISIONS;
	private static final RevisionMode rmDiff           = RevisionMode.UNRESTRICTED_REVISIONS;
	
	private static final int ssisCopy                  = 1800;
	private static final int ssisDiff                  = 1801;

	private static final int ssCopy                    = 67_108_864;
	private static final int ssDiff                    = 67_108_863;

	private static boolean alCopy                      = false;
	private static boolean alDiff                      = true;

	private static ValueRetentionPolicy<?> vrpCopy     = new InvalidatedRetentionPolicy(60);
	private static ValueRetentionPolicy<?> vrpDiff     = new TimeAndVersionRetentionPolicy(Mode.mostRecentValue, 100, 1);

	private static final NamespaceOptions defaultNsOptions     =     NamespaceOptions.templateOptions;
	private static final NamespaceOptions defaultNsOptionsCopy = new NamespaceOptions(stCopy, cpCopy, nsvmCopy, rmCopy, poCopy, ioCopy, goCopy, woCopy, ssisCopy, ssCopy, alCopy, vrpCopy, null);
	private static final NamespaceOptions defaultNsOptionsDiff = new NamespaceOptions(stDiff, cpDiff, nsvmDiff, rmDiff, poDiff, ioDiff, goDiff, woDiff, ssisDiff, ssDiff, alDiff, vrpDiff, null);
	
	private StorageType getStorageType(NamespaceOptions nsOptions) {
		return nsOptions.getStorageType();
	}

	private ConsistencyProtocol getConsistencyProtocol(NamespaceOptions nsOptions) {
		return nsOptions.getConsistencyProtocol();
	}
	
	private NamespaceVersionMode getVersionMode(NamespaceOptions nsOptions) {
		return nsOptions.getVersionMode();
	}
	
	private RevisionMode getRevisionMode(NamespaceOptions nsOptions) {
		return nsOptions.getRevisionMode();
	}
	
	private PutOptions getDefaultPutOptions(NamespaceOptions nsOptions) {
		return nsOptions.getDefaultPutOptions();
	}
	
	private InvalidationOptions getDefaultInvalidationOptions(NamespaceOptions nsOptions) {
		return nsOptions.getDefaultInvalidationOptions();
	}
	
	private GetOptions getDefaultGetOptions(NamespaceOptions nsOptions) {
		return nsOptions.getDefaultGetOptions();
	}
	
	private WaitOptions getDefaultWaitOptions(NamespaceOptions nsOptions) {
		return nsOptions.getDefaultWaitOptions();
	}
	
	private int getSecondarySyncIntervalSeconds(NamespaceOptions nsOptions) {
		return nsOptions.getSecondarySyncIntervalSeconds();
	}
	
	private int getSegmentSize(NamespaceOptions nsOptions) {
		return nsOptions.getSegmentSize();
	}

	private boolean getAllowLinks(NamespaceOptions nsOptions) {
		return nsOptions.getAllowLinks();
	}

	private ValueRetentionPolicy<?> getValueRetentionPolicy(NamespaceOptions nsOptions) {
		return nsOptions.getValueRetentionPolicy();
	}
	
	private NamespaceOptions setStorageType(StorageType st) {
		return defaultNsOptions.storageType(st);
	}
	
	private NamespaceOptions setConsistencyProtocol(ConsistencyProtocol cp) {
		return defaultNsOptions.consistencyProtocol(cp);
	}
	
	private NamespaceOptions setVersionMode(NamespaceVersionMode nsvm) {
		return defaultNsOptions.versionMode(nsvm);
	}
	
	private NamespaceOptions setRevisionMode(RevisionMode rm) {
		return defaultNsOptions.revisionMode(rm);
	}
	
	private NamespaceOptions setDefaultPutOptions(PutOptions po) {
		return defaultNsOptions.defaultPutOptions(po);
	}
	
	private NamespaceOptions setDefaultInvalidationOptions(InvalidationOptions io) {
		return defaultNsOptions.defaultInvalidationOptions(io);
	}
	
	private NamespaceOptions setDefaultGetOptions(GetOptions go) {
		return defaultNsOptions.defaultGetOptions(go);
	}
	
	private NamespaceOptions setDefaultWaitOptions(WaitOptions wo) {
		return defaultNsOptions.defaultWaitOptions(wo);
	}
	
	private NamespaceOptions setSecondarySyncIntervalSeconds(int sssis) {
		return defaultNsOptions.secondarySyncIntervalSeconds(sssis);
	}
	
	private NamespaceOptions setSegmentSize(int ss) {
		return defaultNsOptions.segmentSize(ss);
	}
	
	private NamespaceOptions setAllowLinks(boolean al) {
		return defaultNsOptions.allowLinks(al);
	}
	
	private NamespaceOptions setValueRetentionPolicy(ValueRetentionPolicy<?> vrp) {
		return defaultNsOptions.valueRetentionPolicy(vrp);
	}
	
//	@Test
//	public void testInit() {
//		fail("Not yet implemented");
//	}

	@Test
	public void testGetters() {
		Object[][] testCases = {
			{defaultStorageType,                  getStorageType(defaultNsOptions)},
			{defaultConsistencyProtocol,          getConsistencyProtocol(defaultNsOptions)},
			{defaultVersionMode,                  getVersionMode(defaultNsOptions)},
			{defaultRevisionMode,                 getRevisionMode(defaultNsOptions)},
			{standardPutOptions,                  getDefaultPutOptions(defaultNsOptions)},
			{standardInvalidationOptions,         getDefaultInvalidationOptions(defaultNsOptions)},
			{standardGetOptions,                  getDefaultGetOptions(defaultNsOptions)},
			{standardWaitOptions,                 getDefaultWaitOptions(defaultNsOptions)},
			{defaultSecondarySyncIntervalSeconds, getSecondarySyncIntervalSeconds(defaultNsOptions)},
			{defaultSegmentSize,                  getSegmentSize(defaultNsOptions)},
			{defaultAllowLinks,                   getAllowLinks(defaultNsOptions)},
			{defaultRetentionPolicy,              getValueRetentionPolicy(defaultNsOptions)},
		};
		
		test_Getters(testCases);
	}
	
	// this takes care of testing ctors as well
	@Test
	public void testSetters_Exceptions() {
		Object[][] testCases = {
			{"storageType = null",                      new ExceptionChecker() { @Override public void check() { setStorageType(null);                              } },         NullPointerException.class},
			{"consistencyProtocol = null",              new ExceptionChecker() { @Override public void check() { setConsistencyProtocol(null);                      } },         NullPointerException.class},
			{"versionMode = null",                      new ExceptionChecker() { @Override public void check() { setVersionMode(null);                              } },         NullPointerException.class},
			{"revisionMode = null",                     new ExceptionChecker() { @Override public void check() { setRevisionMode(null);                             } },         NullPointerException.class},
			{"defaultPutOptions = null",                new ExceptionChecker() { @Override public void check() { setDefaultPutOptions(null);                        } },         NullPointerException.class},
			{"defaultPutOptions = invalidationOptions", new ExceptionChecker() { @Override public void check() { setDefaultPutOptions(standardInvalidationOptions); } },     IllegalArgumentException.class},
			{"defaultInvalidationOptions = null",       new ExceptionChecker() { @Override public void check() { setDefaultInvalidationOptions(null);               } },         NullPointerException.class},
			{"defaultGetOptions = null",                new ExceptionChecker() { @Override public void check() { setDefaultGetOptions(null);                        } },         NullPointerException.class},
			{"defaultWaitOptions = null",               new ExceptionChecker() { @Override public void check() { setDefaultWaitOptions(null);                       } },         NullPointerException.class},
			{"segmentSize = min-1",                     new ExceptionChecker() { @Override public void check() { setSegmentSize(minSegmentSize-1);                  } }, ConstraintViolationException.class},
			{"segmentSize = max+1",                     new ExceptionChecker() { @Override public void check() { setSegmentSize(maxSegmentSize+1);                  } }, ConstraintViolationException.class},
			{"valueRetentionPolicy = null",             new ExceptionChecker() { @Override public void check() { setValueRetentionPolicy(null);                     } },         NullPointerException.class},
		};
		
		test_SetterExceptions(testCases);
	}
	
	@Test
	public void testSetters() {
		for (StorageType type : StorageType.values()) 
			check_Getter(type, getStorageType( setStorageType(type) ) );	
		
		for (ConsistencyProtocol protocol : ConsistencyProtocol.values())
			check_Getter(protocol, getConsistencyProtocol( setConsistencyProtocol(protocol) ) );
		
		for (NamespaceVersionMode mode : NamespaceVersionMode.values())
			check_Getter(mode, getVersionMode( setVersionMode(mode) ) );
		
		for (RevisionMode mode : RevisionMode.values())
			check_Getter(mode, getRevisionMode( setRevisionMode(mode) ) );

		for (int val : new int[]{int_minVal, -1, 0, 1, int_maxVal})
			check_Getter(val, getSecondarySyncIntervalSeconds( setSecondarySyncIntervalSeconds(val) ) );

		for (int size : new int[]{minSegmentSize, (minSegmentSize+maxSegmentSize)/2, maxSegmentSize})
			check_Getter(size, getSegmentSize( setSegmentSize(size) ) );
		
		for (boolean val : new boolean[]{false, true})
			check_Getter(val, getAllowLinks( setAllowLinks(val) ) );

		Object[][] testCases = {
			{poDiff,  getDefaultPutOptions( setDefaultPutOptions(poDiff) )},
			{ioDiff,  getDefaultInvalidationOptions( setDefaultInvalidationOptions(ioDiff) )},
			{goDiff,  getDefaultGetOptions( setDefaultGetOptions(goDiff) )},
			{woDiff,  getDefaultWaitOptions( setDefaultWaitOptions(woDiff) )},
			{vrpDiff, getValueRetentionPolicy( setValueRetentionPolicy(vrpDiff) )},
		};

		test_Setters(testCases);
	}
	
	@Test
	public void testWriteOnce() {
		checkIsWriteOnce(defaultNsOptions);
		assertFalse(defaultNsOptionsDiff.isWriteOnce());
		NamespaceOptions nsWriteOnce = defaultNsOptionsDiff.asWriteOnce();
		checkIsWriteOnce(nsWriteOnce);
	}
	
	private void checkIsWriteOnce(NamespaceOptions nso) {
		assertTrue(nso.isWriteOnce());
	}
	
	@Test
	public void testHashCode() {
		checkHashCodeEquals(   defaultNsOptions, defaultNsOptions);
		checkHashCodeEquals(   defaultNsOptions, defaultNsOptionsCopy);
		checkHashCodeNotEquals(defaultNsOptions, defaultNsOptionsDiff);
	}

	@Test
	public void testEqualsObject() {
		Object[][] testCases = {
			{defaultNsOptions,     defaultNsOptions,                          defaultNsOptionsDiff},
			{defaultNsOptionsDiff, defaultNsOptionsDiff,                      defaultNsOptions},
			{defaultNsOptionsCopy, defaultNsOptions,                          defaultNsOptionsDiff},
			{defaultNsOptions,     setStorageType(stCopy),                    setStorageType(stDiff)},
			{defaultNsOptions,     setConsistencyProtocol(cpCopy),            setConsistencyProtocol(cpDiff)},
			{defaultNsOptions,     setVersionMode(nsvmCopy),                  setVersionMode(nsvmDiff)},
			{defaultNsOptions,     setRevisionMode(rmCopy),                   setRevisionMode(rmDiff)},
			{defaultNsOptions,     setDefaultPutOptions(poCopy),              setDefaultPutOptions(poDiff)},
			{defaultNsOptions,     setDefaultInvalidationOptions(ioCopy),     setDefaultInvalidationOptions(ioDiff)},
			{defaultNsOptions,     setDefaultGetOptions(goCopy),              setDefaultGetOptions(goDiff)},
			{defaultNsOptions,     setDefaultWaitOptions(woCopy),             setDefaultWaitOptions(woDiff)},
			{defaultNsOptions,     setSecondarySyncIntervalSeconds(ssisCopy), setSecondarySyncIntervalSeconds(ssisDiff)},
			{defaultNsOptions,     setSegmentSize(ssCopy),                    setSegmentSize(ssDiff)},
			{defaultNsOptions,     setAllowLinks(alCopy),                     setAllowLinks(alDiff)},
			{defaultNsOptions,     setValueRetentionPolicy(vrpCopy),          setValueRetentionPolicy(vrpDiff)},
		};
		
		test_FirstEqualsSecond_SecondNotEqualsThird(testCases);
		test_NotEquals(new Object[][]{
			{defaultNsOptions, NamespacePerspectiveOptions.templateOptions},
		});
	}
	
//	@Test
//	public void testCheckOptions() {
//		checkTimeoutControllerForValidity
//	}
	
	@Test
	public void testToStringAndParse() {
		NamespaceOptions[] testCases = {
			defaultNsOptions,
			defaultNsOptionsCopy,
			defaultNsOptionsDiff,
		};
		
		for (NamespaceOptions testCase : testCases)
			checkStringAndParse(testCase);
	}
	
	private void checkStringAndParse(NamespaceOptions nsOptions) {
		assertEquals(nsOptions, NamespaceOptions.parse( nsOptions.toString() ));
	}
}
