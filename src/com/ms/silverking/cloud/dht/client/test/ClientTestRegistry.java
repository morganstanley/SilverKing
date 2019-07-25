package com.ms.silverking.cloud.dht.client.test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class ClientTestRegistry {
    public static List<ClientTest> getTests(Collection<String> names) {
        List<ClientTest>    tests;
        
        tests = new ArrayList<>();
        for (String testName : ImmutableSet.copyOf(names)) {
            tests.add(getTest(testName));
        }
        return ImmutableList.copyOf(tests);
    }
    
    public static ClientTest getTest(String name) {
        switch (name) {
        case LockTest.testName: return new LockTest();
        case RequiredPreviousVersionTest.testName: return new RequiredPreviousVersionTest();
        default: return null;
        }
    }

    public static List<String> getAllTestNames() {
        return ImmutableList.of(LockTest.testName, RequiredPreviousVersionTest.testName);
    }
    
    public static List<ClientTest> getAllTests() {
        List<ClientTest>    tests;
        
        tests = new ArrayList<>();
        for (String testName : getAllTestNames()) {
            tests.add(getTest(testName));
        }
        return ImmutableList.copyOf(tests);
    }
}
