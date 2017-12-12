#include "MultiPutMultiGetTest.h"

#include "TestUtil.h"

TEST_F(MultiPutMultiGetTest, PutGetHelloWorld) {
	TestUtil::initAndTest(putGetHelloWorld);

	ASSERT_NOT_NULL(putGetHelloWorld->getSyncNSPerspective());
	if (putGetHelloWorld->isValueVersionSet()) {
		ASSERT_NOT_NULL(putGetHelloWorld->getGetOptions());
	}

	vector<string> keys = putGetHelloWorld->getKeys();
	vector<string> vals = putGetHelloWorld->getValues();
	TestUtil::checkKeysAndValues(keys, vals);
	
	// cout << "NSP: " << putGetHelloWorld->getNamespaceOptions()->toString() << endl;
	// cout << endl;
	// cout << "NSPO: " << putGetHelloWorld->getNSPOptions()->toString() << endl;
	
	map<string, string> keysAndVals = putGetHelloWorld->getKeyVals(); 
	if (!putGetHelloWorld->isCompressSet()) {
		// cout << "NOT compressSet" << endl;
		putGetHelloWorld->put(keys, vals);
	}
	else {
		// cout << "compressSet" << endl;
		// cout << "PutOpt: " << putGetHelloWorld->getPutOpt()->toString() << endl;
		putGetHelloWorld->put(keysAndVals, putGetHelloWorld->getPutOpt());
	}
	
	map<string, string> storedKeysValues;
	if (!putGetHelloWorld->isValueVersionSet()) {	
		// cout << "NOT valueVersionSet" << endl;
		storedKeysValues = putGetHelloWorld->get(keys); // values stored in SK
	}
	else {
		ASSERT_NOT_NULL(putGetHelloWorld->getGetOpt());
		// cout << "valueVersionSet" << endl;
		// cout << "GetOpt: " << putGetHelloWorld->getGetOpt()->toString() << endl;
		storedKeysValues = putGetHelloWorld->get(keys, putGetHelloWorld->getGetOpt()); // values stored in SK
	}
	
	
	int numKeys = putGetHelloWorld->getNumOfKeys();
	TestUtil::checkKeysAndValues(numKeys, keys, keysAndVals, storedKeysValues);
}

int main(int argc, char ** argv) {
	::testing::InitGoogleTest(&argc, argv);	// important that this is before parseCmdLine because there are some gtest options that mess up parseCmdLine, i.e. those starting with "--"
	CmdLineOptions o = Util::parseCmdLine(argc, argv);
	putGetHelloWorld = new MultiPutMultiGet(o.gcName, o.host, o.ns, o.logfile, o.verbose, o.nsOptions, o.jvmOptions, o.compressType, o.valueVersion, o.retrievalType, o.key, o.value, 3);

	return RUN_ALL_TESTS(); 
}
