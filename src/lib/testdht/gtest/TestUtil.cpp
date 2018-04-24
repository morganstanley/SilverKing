
#include "TestUtil.h"

#include "gtest/gtest.h"

const string TestUtil::HELLO_KEY   = "Hello";
const string TestUtil::WORLD_VALUE = "World";
const int TestUtil::NUM_KEYS = 3;

void TestUtil::initAndTest(DhtAction * pActionObject) {
	ASSERT_TRUE(    pActionObject->initClient());
	ASSERT_NOT_NULL(pActionObject->getClient());
	ASSERT_NOT_NULL(pActionObject->getGridConfiguration());
	ASSERT_NOT_NULL(pActionObject->getClientDHTConfiguration());
	ASSERT_NOT_NULL(pActionObject->getSessionOptions());
	ASSERT_NOT_NULL(pActionObject->openSession());
    ASSERT_NOT_NULL(pActionObject->getSession());
	ASSERT_NULL(    pActionObject->getNamespaceOpts());
	ASSERT_NOT_NULL(pActionObject->getNamespaceOptions());
	ASSERT_NOT_NULL(pActionObject->getNamespaceOpts());
	ASSERT_FALSE(   pActionObject->getNamespaceOptStr().empty());
	ASSERT_FALSE(   pActionObject->getNamespaceName().empty());
	ASSERT_NOT_NULL(pActionObject->getNamespace());
	ASSERT_NOT_NULL(pActionObject->getNSPOptions());
	
	if (pActionObject->isCompressSet()) {
		ASSERT_NOT_NULL(pActionObject->getPutOptions());
		ASSERT_NOT_NULL(pActionObject->getPutOpt());
	}
}

void TestUtil::checkKeyAndValue(string k, string v) {
	ASSERT_EQ(TestUtil::HELLO_KEY,   k);
	ASSERT_EQ(TestUtil::WORLD_VALUE, v);
}

void TestUtil::checkKeysAndValues(vector<string> keys, vector<string> vals) {
	ASSERT_EQ(TestUtil::NUM_KEYS, keys.size());
	ASSERT_EQ(TestUtil::NUM_KEYS, vals.size());
	
	string k = TestUtil::HELLO_KEY;
	string v = TestUtil::WORLD_VALUE;
	for (int i = 0; i < TestUtil::NUM_KEYS; i++) {
		string key(string(k).append("_").append(to_string(i)));
		string val(string(v).append("_").append(to_string(i)));
		ASSERT_EQ(key, keys[i]);
		ASSERT_EQ(val, vals[i]);
	}
}

void TestUtil::checkKeysAndValues(int numKeys, vector<string> keys, map<string, string> expectedKeysValues, map<string, string> storedKeysValues) {
	for (int i = 0; i < numKeys; i++) {
		string key = keys[i];
		EXPECT_EQ(expectedKeysValues[key], storedKeysValues[key]);
	}
}

void TestUtil::checkMetaIsntEmpty(int numKeys, vector<string> keys, map<string, string> storedKeysValues) {
	for (int i = 0; i < numKeys; i++) {
		EXPECT_FALSE( storedKeysValues[ keys[i] ].empty() );
	}
}
		
