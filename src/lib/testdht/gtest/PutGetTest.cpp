#include "PutGetTest.h"

#include "TestUtil.h"

PutGetTest:: PutGetTest() {}
PutGetTest::~PutGetTest() { delete putGetHelloWorld; }

TEST_F(PutGetTest, PutGetHelloWorld) {
	TestUtil::initAndTest(putGetHelloWorld);
				
	ASSERT_NOT_NULL(putGetHelloWorld->getSyncNSPerspective());
	
	if (putGetHelloWorld->isValueVersionSet()) {
		ASSERT_NOT_NULL(putGetHelloWorld->getGetOptions());
	}
	
	string k = putGetHelloWorld->getKey();
	string v = putGetHelloWorld->getValue();
	TestUtil::checkKeyAndValue(k, v);
	
	if(!putGetHelloWorld->isCompressSet()) {
		ASSERT_NO_THROW(putGetHelloWorld->put(k, v));
	}
	else {
		putGetHelloWorld->put(k + "_" + putGetHelloWorld->getCompression(), v + "_" + putGetHelloWorld->getCompression(), putGetHelloWorld->getPutOpt());
	}

	if(!putGetHelloWorld->isValueVersionSet()) {
		EXPECT_EQ(v, putGetHelloWorld->get(k));
	}
	else {
		SKGetOptions* pGetOpt = putGetHelloWorld->getGetOpt();
		ASSERT_NOT_NULL(pGetOpt);
		EXPECT_EQ(v, putGetHelloWorld->get(k, pGetOpt));
	}
}

int main(int argc, char ** argv) {
	::testing::InitGoogleTest(&argc, argv);	// important that this is before parseCmdLine because there are some gtest options that mess up parseCmdLine, i.e. those starting with "--"
	CmdLineOptions o = Util::parseCmdLine(argc, argv);
	putGetHelloWorld = new PutGet(o.gcName, o.host, o.ns, o.logfile, o.verbose, o.nsOptions, o.jvmOptions, o.compressType, o.valueVersion, o.retrievalType, o.key, o.value);

	return RUN_ALL_TESTS(); 
}
