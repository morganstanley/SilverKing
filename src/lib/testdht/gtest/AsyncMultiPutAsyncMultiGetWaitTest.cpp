#include "AsyncMultiPutAsyncMultiGetWaitTest.h"

#include "TestUtil.h"

TEST_F(AsyncMultiPutAsyncMultiGetWaitTest, PutGetHelloWorld) {
	TestUtil::initAndTest(putGetHelloWorld);

	ASSERT_NOT_NULL(putGetHelloWorld->getAsyncNSPerspective());
	ASSERT_NOT_NULL(putGetHelloWorld->getWaitOptions());

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

	ASSERT_NOT_NULL(putGetHelloWorld->getWaitOpt());
	map<string, string> storedKeysValues = putGetHelloWorld->waitFor(keys, putGetHelloWorld->getWaitOpt()); // values stored in SK
	int numKeys = putGetHelloWorld->getNumOfKeys();
	TestUtil::checkKeysAndValues(numKeys, keys, keysAndVals, storedKeysValues);
}

int main(int argc, char ** argv) {
	::testing::InitGoogleTest(&argc, argv);	// important that this is before parseCmdLine because there are some gtest options that mess up parseCmdLine, i.e. those starting with "--"
	CmdLineOptions o = Util::parseCmdLine(argc, argv);
	putGetHelloWorld = new AsyncMultiPutAsyncMultiGetWait(o.gcName, o.host, o.ns, o.logfile, o.verbose, o.nsOptions, o.jvmOptions, o.compressType, o.valueVersion, o.retrievalType, o.key, o.value, 3, o.timeout, o.threshold);

	return RUN_ALL_TESTS(); 
}
