package com.ms.silverking.cloud.dht.common;

import com.ms.silverking.cloud.dht.ConsistencyProtocol;
import com.ms.silverking.cloud.dht.LRURetentionPolicy;
import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.NamespaceServerSideCode;
import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.RevisionMode;
import com.ms.silverking.cloud.dht.StorageType;
import com.ms.silverking.cloud.dht.daemon.storage.serverside.LRUTrigger;
import org.junit.Test;

import static com.ms.silverking.testing.Util.getTestMessage;
import static org.junit.Assert.*;

public class NamespacePropertiesReflectionTest {
    private static final long nanosPerMilli = 1000000;

    final private NamespaceOptions dummyNsOptions = new NamespaceOptions(
            StorageType.FILE,
            ConsistencyProtocol.LOOSE,
            NamespaceVersionMode.SYSTEM_TIME_NANOS,
            RevisionMode.NO_REVISIONS,
            NamespaceUtil.metaNSDefaultPutOptions,
            NamespaceUtil.metaNSDefaultInvalidationOptions,
            NamespaceUtil.metaNSDefaultGetOptions,
            DHTConstants.standardWaitOptions,
            1996,
            4096,
            1996,
            false,
            DHTConstants.defaultStorageFormat,
            new LRURetentionPolicy(8L * 1024L * 1024L, 1),
            NamespaceServerSideCode.singleTrigger(LRUTrigger.class)
    );

    final private String dummyParentName = "SteelBallRun";
    final private String dummyNsName = "StoneFree";
    final private long dummyCreationTime = System.currentTimeMillis() * nanosPerMilli;
    final private long dummyMinVersion = 1996;

    @Test
    public void reflectionFull() {
        NamespaceProperties initialized = new NamespaceProperties(dummyNsOptions, dummyNsName, dummyParentName, dummyMinVersion, dummyCreationTime);

        String skDef = initialized.toString();
        NamespaceProperties parsed = NamespaceProperties.parse(skDef);
        String testName = "reflectionFull";
        assertTrue(getTestMessage(testName, "reflection.hasCreationTime() shall be true for full initialization"), initialized.hasCreationTime());
        assertTrue(getTestMessage(testName, "reflection.hasCreationTime() shall be true for full initialization"), parsed.hasCreationTime());
        assertTrue(getTestMessage(testName, "reflection.hasName() shall be true for full initialization"), initialized.hasName());
        assertTrue(getTestMessage(testName, "reflection.hasName() shall be true for full initialization"), parsed.hasName());
        assertEquals(getTestMessage(testName, "reflection shall work for full initialization"), initialized, parsed);
        assertEquals(getTestMessage(testName, "reflection.toString shall work for full initialization"), skDef, parsed.toString());
    }

    @Test
    public void reflectionNoCreationTime() {
        NamespaceProperties initialized = (new NamespaceProperties(dummyNsOptions)).name(dummyNsName);

        String skDef = initialized.toString();
        NamespaceProperties parsed = NamespaceProperties.parse(skDef);
        String testName = "reflectionNoCreationTime";
        assertFalse(getTestMessage(testName, "reflection.hasCreationTime() shall be false for partial initialization"), initialized.hasCreationTime());
        assertFalse(getTestMessage(testName, "reflection.hasCreationTime() shall be false for partial initialization"), parsed.hasCreationTime());
        assertTrue(getTestMessage(testName, "reflection.hasName() shall be true for partial initialization"), initialized.hasName());
        assertTrue(getTestMessage(testName, "reflection.hasName() shall be true for partial initialization"), parsed.hasName());
        assertEquals(getTestMessage(testName, "reflection shall work for partial initialization"), initialized, parsed);
        assertEquals(getTestMessage(testName, "reflection.toString shall work for partial initialization"), skDef, parsed.toString());
    }

    @Test
    public void reflectionNoName() {
        NamespaceProperties initialized = (new NamespaceProperties(dummyNsOptions)).creationTime(dummyCreationTime);

        String skDef = initialized.toString();
        NamespaceProperties parsed = NamespaceProperties.parse(skDef);
        String testName = "reflectionNoName";
        assertTrue(getTestMessage(testName, "reflection.hasCreationTime() shall be true for partial initialization"), initialized.hasCreationTime());
        assertTrue(getTestMessage(testName, "reflection.hasCreationTime() shall be true for partial initialization"), parsed.hasCreationTime());
        assertFalse(getTestMessage(testName, "reflection.hasName() shall be false for partial initialization"), initialized.hasName());
        assertFalse(getTestMessage(testName, "reflection.hasName() shall be false for partial initialization"), parsed.hasName());
        assertEquals(getTestMessage(testName, "reflection shall work for partial initialization"), initialized, parsed);
        assertEquals(getTestMessage(testName, "reflection.toString shall work for partial initialization"), skDef, parsed.toString());
    }

    @Test
    public void reflectionNoCreationTimeNoName1() {
        NamespaceProperties initialized = new NamespaceProperties(dummyNsOptions);

        String skDef = initialized.toString();
        NamespaceProperties parsed = NamespaceProperties.parse(skDef);
        String testName = "reflectionNoCreationTimeNoName1";
        assertFalse(getTestMessage(testName, "reflection.hasCreationTime() shall be false for partial initialization"), initialized.hasCreationTime());
        assertFalse(getTestMessage(testName, "reflection.hasCreationTime() shall be false for partial initialization"), parsed.hasCreationTime());
        assertFalse(getTestMessage(testName, "reflection.hasName() shall be false for partial initialization"), initialized.hasName());
        assertFalse(getTestMessage(testName, "reflection.hasName() shall be false for partial initialization"), parsed.hasName());
        assertEquals(getTestMessage(testName, "reflection shall work for partial initialization"), initialized, parsed);
        assertEquals(getTestMessage(testName, "reflection.toString shall work for partial initialization"), skDef, parsed.toString());
    }


    @Test
    public void reflectionNoCreationTimeNoName2() {
        NamespaceProperties initialized = new NamespaceProperties(dummyNsOptions, dummyParentName, 1);

        String skDef = initialized.toString();
        NamespaceProperties parsed = NamespaceProperties.parse(skDef);
        String testName = "reflectionNoCreationTimeNoName2";
        assertFalse(getTestMessage(testName, "reflection.hasCreationTime() shall be false for partial initialization"), initialized.hasCreationTime());
        assertFalse(getTestMessage(testName, "reflection.hasCreationTime() shall be false for partial initialization"), parsed.hasCreationTime());
        assertFalse(getTestMessage(testName, "reflection.hasName() shall be false for partial initialization"), initialized.hasName());
        assertFalse(getTestMessage(testName, "reflection.hasName() shall be false for partial initialization"), parsed.hasName());
        assertEquals(getTestMessage(testName, "reflection shall work for partial initialization"), initialized, parsed);
        assertEquals(getTestMessage(testName, "reflection.toString shall work for partial initialization"), skDef, parsed.toString());
    }

    @Test
    public void canParsePossibleSKDef() {
        // Possible Sk def which contains"name"
        String possibleSKDef = "options={storageType=FILE,consistencyProtocol=LOOSE,versionMode=SYSTEM_TIME_NANOS,revisionMode=NO_REVISIONS," +
                "defaultPutOptions={opTimeoutController=<OpSizeBasedTimeoutController>{maxAttempts=4,constantTime_ms=300000,itemTime_ms=305,nonKeyedOpMaxRelTimeout_ms=1500000,exclusionChangeRetryInterval_ms=5000}," +
                "compression=NONE,checksumType=MD5,checksumCompressedValues=false,version=0,requiredPreviousVersion=0,lockSeconds=0,fragmentationThreshold=10485760,}," +
                "defaultInvalidationOptions={opTimeoutController=<OpSizeBasedTimeoutController>{maxAttempts=4,constantTime_ms=300000,itemTime_ms=305,nonKeyedOpMaxRelTimeout_ms=1500000,exclusionChangeRetryInterval_ms=5000}," +
                "compression=NONE,checksumType=SYSTEM,checksumCompressedValues=false,version=0,requiredPreviousVersion=0,lockSeconds=0,fragmentationThreshold=10485760,}," +
                "defaultGetOptions={opTimeoutController=<OpSizeBasedTimeoutController>{maxAttempts=4,constantTime_ms=300000,itemTime_ms=305,nonKeyedOpMaxRelTimeout_ms=1500000,exclusionChangeRetryInterval_ms=5000}," +
                "retrievalType=VALUE,waitMode=GET,versionConstraint={min=-9223372036854775808,max=9223372036854775807,mode=GREATEST,maxCreationTime=9223372036854775807},nonExistenceResponse=NULL_VALUE," +
                "verifyChecksums=true,returnInvalidations=false,forwardingMode=ALL,updateSecondariesOnMiss=false,}," +
                "defaultWaitOptions={opTimeoutController=<WaitForTimeoutController>{internalRetryIntervalSeconds=20,internalExclusionChangeRetryIntervalSeconds=2},retrievalType=VALUE,waitMode=WAIT_FOR," +
                "versionConstraint={min=-9223372036854775808,max=9223372036854775807,mode=GREATEST,maxCreationTime=9223372036854775807},nonExistenceResponse=NULL_VALUE,verifyChecksums=true,returnInvalidations=false," +
                "forwardingMode=FORWARD,updateSecondariesOnMiss=false,timeoutSeconds=2147483647,threshold=100,timeoutResponse=EXCEPTION},secondarySyncIntervalSeconds=1996,segmentSize=4096,maxValueSize=1996,allowLinks=false," +
                "valueRetentionPolicy=<LRURetentionPolicy>{capacityBytes=8388608,maxVersions=1},namespaceServerSideCode={url=," +
                "putTrigger=com.ms.silverking.cloud.dht.daemon.storage.serverside.LRUTrigger,retrieveTrigger=com.ms.silverking.cloud.dht.daemon.storage.serverside.LRUTrigger}}," +
                "parent=SteelBallRun," +
                "minVersion=1," +
                "name=" + dummyNsName;

        NamespaceProperties parsed = NamespaceProperties.parse(possibleSKDef);
        String testName = "canParsePossibleSkDef";
        assertFalse(getTestMessage(testName, "reflection.hasCreationTime() shall be false for partial initialization"), parsed.hasCreationTime());
        assertEquals(getTestMessage(testName, "reflection.getName() shall be correctly reflected"), dummyNsName, parsed.getName());
    }

    @Test
    public void canParseLegacySKDef() {
        // Legacy Sk def which contains no "name" and no "creationTime" (same format as current production env)
        String legacySKDef = "options={storageType=FILE,consistencyProtocol=LOOSE,versionMode=SYSTEM_TIME_NANOS,revisionMode=NO_REVISIONS," +
                "defaultPutOptions={opTimeoutController=<OpSizeBasedTimeoutController>{maxAttempts=4,constantTime_ms=300000,itemTime_ms=305,nonKeyedOpMaxRelTimeout_ms=1500000,exclusionChangeRetryInterval_ms=5000}," +
                "compression=NONE,checksumType=MD5,checksumCompressedValues=false,version=0,requiredPreviousVersion=0,lockSeconds=0,fragmentationThreshold=10485760,}," +
                "defaultInvalidationOptions={opTimeoutController=<OpSizeBasedTimeoutController>{maxAttempts=4,constantTime_ms=300000,itemTime_ms=305,nonKeyedOpMaxRelTimeout_ms=1500000,exclusionChangeRetryInterval_ms=5000}," +
                "compression=NONE,checksumType=SYSTEM,checksumCompressedValues=false,version=0,requiredPreviousVersion=0,lockSeconds=0,fragmentationThreshold=10485760,}," +
                "defaultGetOptions={opTimeoutController=<OpSizeBasedTimeoutController>{maxAttempts=4,constantTime_ms=300000,itemTime_ms=305,nonKeyedOpMaxRelTimeout_ms=1500000,exclusionChangeRetryInterval_ms=5000}," +
                "retrievalType=VALUE,waitMode=GET,versionConstraint={min=-9223372036854775808,max=9223372036854775807,mode=GREATEST,maxCreationTime=9223372036854775807},nonExistenceResponse=NULL_VALUE," +
                "verifyChecksums=true,returnInvalidations=false,forwardingMode=ALL,updateSecondariesOnMiss=false,}," +
                "defaultWaitOptions={opTimeoutController=<WaitForTimeoutController>{internalRetryIntervalSeconds=20,internalExclusionChangeRetryIntervalSeconds=2},retrievalType=VALUE,waitMode=WAIT_FOR," +
                "versionConstraint={min=-9223372036854775808,max=9223372036854775807,mode=GREATEST,maxCreationTime=9223372036854775807},nonExistenceResponse=NULL_VALUE,verifyChecksums=true,returnInvalidations=false," +
                "forwardingMode=FORWARD,updateSecondariesOnMiss=false,timeoutSeconds=2147483647,threshold=100,timeoutResponse=EXCEPTION},secondarySyncIntervalSeconds=1996,segmentSize=4096,maxValueSize=1996,allowLinks=false," +
                "valueRetentionPolicy=<LRURetentionPolicy>{capacityBytes=8388608,maxVersions=1},namespaceServerSideCode={url=," +
                "putTrigger=com.ms.silverking.cloud.dht.daemon.storage.serverside.LRUTrigger,retrieveTrigger=com.ms.silverking.cloud.dht.daemon.storage.serverside.LRUTrigger}}," +
                "parent=SteelBallRun," +
                "minVersion=1";

        NamespaceProperties parsed = NamespaceProperties.parse(legacySKDef);
        String testName = "canParseLegacySkDef";
        assertFalse(getTestMessage(testName, "reflection.hasCreationTime() shall be false for partial initialization"), parsed.hasCreationTime());
        assertFalse(getTestMessage(testName, "reflection.hasName() shall be false for partial initialization"), parsed.hasName());
    }

    @Test
    public void backwardCompatibilityFunctionalityFull() {
        NamespaceProperties initialized = new NamespaceProperties(dummyNsOptions, dummyNsName, dummyParentName, dummyMinVersion, dummyCreationTime);
        String advancedDef = initialized.toString();
        String legacyDef = initialized.toLegacySKDef();

        String[] doggyFields = { "creationTime=", "name=" };

        NamespaceProperties advancedParsed = NamespaceProperties.parse(advancedDef);
        NamespaceProperties legacyParsed = NamespaceProperties.parse(legacyDef);

        String testName = "backwardCompatibility";
        assertNotEquals(getTestMessage(testName, "legacyDef shall drop new fields"), advancedDef, legacyDef);
        for (String doggyField : doggyFields) {
            assertFalse(getTestMessage(testName, "legacyDef shall drop ["+ doggyField + "] field"), legacyDef.contains(doggyField));
        }
        for (String doggyField : doggyFields) {
            assertTrue(getTestMessage(testName, "advancedDef shall keep ["+ doggyField + "] field"), advancedDef.contains(doggyField));
        }

        assertFalse(getTestMessage(testName, "legacyDef shall drop [creationTime] field and successfully parsed"), legacyParsed.hasCreationTime());
        assertFalse(getTestMessage(testName, "legacyDef shall drop [name] field and successfully parsed"), legacyParsed.hasName());
        assertTrue(getTestMessage(testName, "Non-doggy fields shall be same between advanced and legacy"), legacyParsed.getParent().equals(advancedParsed.getParent()));
        assertTrue(getTestMessage(testName, "Non-doggy fields shall be same between advanced and legacy"), legacyParsed.getOptions().equals(advancedParsed.getOptions()));
        assertTrue(getTestMessage(testName, "Non-doggy fields shall be same between advanced and legacy"), legacyParsed.getMinVersion() == advancedParsed.getMinVersion());
    }

    @Test
    public void backwardCompatibilityFunctionalityNoName() {
        NamespaceProperties initialized = new NamespaceProperties(dummyNsOptions, dummyParentName, dummyMinVersion).creationTime(dummyCreationTime);
        String advancedDef = initialized.toString();
        String legacyDef = initialized.toLegacySKDef();

        String[] doggyFields = { "creationTime=" };

        NamespaceProperties advancedParsed = NamespaceProperties.parse(advancedDef);
        NamespaceProperties legacyParsed = NamespaceProperties.parse(legacyDef);

        String testName = "backwardCompatibility";
        assertNotEquals(getTestMessage(testName, "legacyDef shall drop new fields"), advancedDef, legacyDef);
        for (String doggyField : doggyFields) {
            assertFalse(getTestMessage(testName, "legacyDef shall drop ["+ doggyField + "] field"), legacyDef.contains(doggyField));
        }
        for (String doggyField : doggyFields) {
            assertTrue(getTestMessage(testName, "advancedDef shall keep ["+ doggyField + "] field"), advancedDef.contains(doggyField));
        }

        assertFalse(getTestMessage(testName, "legacyDef shall drop [creationTime] field and successfully parsed"), legacyParsed.hasCreationTime());
        assertFalse(getTestMessage(testName, "legacyDef shall drop [name] field and successfully parsed"), legacyParsed.hasName());
        assertTrue(getTestMessage(testName, "Non-doggy fields shall be same between advanced and legacy"), legacyParsed.getParent().equals(advancedParsed.getParent()));
        assertTrue(getTestMessage(testName, "Non-doggy fields shall be same between advanced and legacy"), legacyParsed.getOptions().equals(advancedParsed.getOptions()));
        assertTrue(getTestMessage(testName, "Non-doggy fields shall be same between advanced and legacy"), legacyParsed.getMinVersion() == advancedParsed.getMinVersion());
    }
}
