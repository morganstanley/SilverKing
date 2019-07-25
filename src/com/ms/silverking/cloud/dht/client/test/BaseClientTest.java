package com.ms.silverking.cloud.dht.client.test;

import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.RevisionMode;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;

public abstract class BaseClientTest implements ClientTest {
    private final String                testName;
    private final NamespaceVersionMode  nsVersionMode;
    private final RevisionMode          revisionMode;
    
    public BaseClientTest(String testName, NamespaceVersionMode nsVersionMode, RevisionMode revisionMode) {
        this.testName = testName;
        this.nsVersionMode = nsVersionMode;
        this.revisionMode = revisionMode;
    }

    public BaseClientTest(String testName) {
        this(testName, NamespaceVersionMode.SYSTEM_TIME_NANOS, RevisionMode.NO_REVISIONS);
    }
    
    @Override
    public String getTestName() {
        return testName;
    }

    @Override
    public NamespaceVersionMode getNamespaceVersionMode() {
        return nsVersionMode;
    }

    @Override
    public RevisionMode getRevisionMode() {
        return revisionMode;
    }
    
    protected void checkValue(SynchronousNamespacePerspective<String,String> syncNSP, String key, String expectedValue) throws RetrievalException {
        String  value;
        
        System.out.printf("Reading %s\n", key);
        value = syncNSP.get(key);
        System.out.printf("value: %s\n", value);
        if (!value.equals(expectedValue)) {
            throw new RuntimeException(String.format("Expected: %s\tFound: %s", expectedValue, value));
        }
    }
}
