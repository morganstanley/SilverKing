#include "AsyncMultiPutAsyncMultiGetMetaTest.h"

#include "TestUtil.h"

TEST_F(AsyncMultiPutAsyncMultiGetMetaTest, PutGetHelloWorld) {
	TestUtil::initAndTest(putGetHelloWorld);
	
	ASSERT_NOT_NULL(putGetHelloWorld->getAsyncNSPerspective());
	ASSERT_NOT_NULL(putGetHelloWorld->getMetaGetOptions());

	vector<string> keys = putGetHelloWorld->getKeys();
	vector<string> vals = putGetHelloWorld->getValues();
	TestUtil::checkKeysAndValues(keys, vals);
	
	map<string, string> keysAndVals = putGetHelloWorld->getKeyVals(); 
	if (!putGetHelloWorld->isCompressSet()) {
		putGetHelloWorld->put(keys, vals);
	}
	else {
		putGetHelloWorld->put(keysAndVals, putGetHelloWorld->getPutOpt());
	}

	ASSERT_NOT_NULL(putGetHelloWorld->getMetaGetOpt());
	map<string, string> storedKeysValues = putGetHelloWorld->getMeta(keys, putGetHelloWorld->getMetaGetOpt()); // values stored in SK
	int numKeys = putGetHelloWorld->getNumOfKeys();
	TestUtil::checkMetaIsntEmpty(numKeys, keys, storedKeysValues);
}

int main(int argc, char ** argv) {
	::testing::InitGoogleTest(&argc, argv);	// important that this is before parseCmdLine because there are some gtest options that mess up parseCmdLine, i.e. those starting with "--"
	CmdLineOptions o = Util::parseCmdLineMeta(argc, argv);
	putGetHelloWorld = new AsyncMultiPutAsyncMultiGetMeta(o.gcName, o.host, o.ns, o.logfile, o.verbose, o.nsOptions, o.jvmOptions, o.compressType, o.valueVersion, o.retrievalType, o.key, o.value, 3, o.timeout);

	return RUN_ALL_TESTS(); 
}
