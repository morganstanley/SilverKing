#ifndef TEST_UTIL_H
#define TEST_UTIL_H

#define ASSERT_NULL(X)     ASSERT_TRUE(X == NULL)
#define ASSERT_NOT_NULL(X) ASSERT_TRUE(X != NULL)

#include "DhtAction.h"
#include <string>

class TestUtil {
	public:
		static void initAndTest(DhtAction * pActionObject);
		static void checkKeyAndValue(string k, string v);
		static void checkKeysAndValues(vector<string> keys, vector<string> vals);
		static void checkKeysAndValues(int numKeys, vector<string> keys, map<string, string> expectedKeysValues, map<string, string> storedKeysValues);
		static void checkMetaIsntEmpty(int numKeys, vector<string> keys, map<string, string> storedKeysValues);
		
		static const string HELLO_KEY;
		static const string WORLD_VALUE;
		static const int NUM_KEYS;
};

#endif
