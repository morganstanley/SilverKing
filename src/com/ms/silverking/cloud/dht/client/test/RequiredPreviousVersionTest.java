package com.ms.silverking.cloud.dht.client.test;

import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.RevisionMode;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.FailureCause;
import com.ms.silverking.cloud.dht.client.Namespace;
import com.ms.silverking.cloud.dht.client.PutException;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.log.Log;

public class RequiredPreviousVersionTest extends BaseClientTest {
    public static final String testName = "RequiredPreviousVersionTest";
    
    private static final String testKey = "k1";
    private static final String initialValue = "v1.1";
    private static final String failedPutValue = "v1.2";
    private static final String modifiedValue = "v1.3";
    private static final String modifiedValue2 = "v1.4";
    
    public RequiredPreviousVersionTest() {
        super(testName, NamespaceVersionMode.CLIENT_SPECIFIED, RevisionMode.NO_REVISIONS);
    }

    @Override
    public Pair<Integer,Integer> runTest(DHTSession session, Namespace ns) {
        SynchronousNamespacePerspective<String,String>  syncNSP;
        
        syncNSP = ns.openSyncPerspective(ns.getDefaultNSPOptions(String.class, String.class));
        return testRequiredPreviousVersion(syncNSP);
    }
    
    public Pair<Integer,Integer> testRequiredPreviousVersion(SynchronousNamespacePerspective<String,String> syncNSP) {
        int successful;
        int failed;
        
        successful = 0;
        failed = 0;
        try {
            PutOptions  defaultPutOptions;
            
            defaultPutOptions = syncNSP.getNamespace().getOptions().getDefaultPutOptions();
            
            System.out.println("Writing");
            syncNSP.put(testKey, initialValue,  defaultPutOptions.version(1));
            
            // Test the advisory lock
            System.out.println("Writing - should fail due to version <= requiredPreviousVersion");
            try {
                syncNSP.put(testKey, failedPutValue, defaultPutOptions.requiredPreviousVersion(2).version(2));
                throw new RuntimeException("Failed to generate IllegalArgumentException");
            } catch (IllegalArgumentException iae) {
                // Expected
                System.out.println("Correctly generated IllegalArgumentException");
            }
            checkValue(syncNSP, testKey, initialValue);
            
            System.out.println("Writing - should fail due to requiredPreviousVersion not met");
            try {
                syncNSP.put(testKey, failedPutValue, defaultPutOptions.requiredPreviousVersion(5).version(6));
            } catch (PutException pe) {
                if (pe.getFailureCause(testKey) == FailureCause.INVALID_VERSION) {
                    System.out.println("Correctly detected invalid version");
                } else {
                    throw new RuntimeException("Unexpected failure", pe);
                }
            }
            checkValue(syncNSP, testKey, initialValue);
            
            // Test requiredPreviousVersion
            syncNSP.put(testKey, modifiedValue, defaultPutOptions.requiredPreviousVersion(1).version(2));
            checkValue(syncNSP, testKey, modifiedValue);

            // Test requiredPreviousVersion
            syncNSP.put(testKey, modifiedValue2, defaultPutOptions.requiredPreviousVersion(2).version(3));
            checkValue(syncNSP, testKey, modifiedValue2);
            
            ++successful;
        } catch (Exception e) {
            Log.logErrorWarning(e, "testRequiredPreviousVersion failed");
            ++failed;
        }
        return Pair.of(successful, failed);
    }
}
