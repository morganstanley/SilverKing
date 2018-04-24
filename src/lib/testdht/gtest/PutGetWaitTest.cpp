#include "PutGetWaitTest.h"

#include "TestUtil.h"

PutGetWaitTest:: PutGetWaitTest() {}
PutGetWaitTest::~PutGetWaitTest() { delete putGetHelloWorld; }

TEST_F(PutGetWaitTest, PutGetHelloWorld) {
	TestUtil::initAndTest(putGetHelloWorld);
	
	ASSERT_NOT_NULL(putGetHelloWorld->getSyncNSPerspective());
	ASSERT_NOT_NULL(putGetHelloWorld->getWaitOptions());
	
	string k = putGetHelloWorld->getKey();
	string v = putGetHelloWorld->getValue();
	TestUtil::checkKeyAndValue(k, v);
	
	if(!putGetHelloWorld->isCompressSet()) {
		ASSERT_NO_THROW(putGetHelloWorld->put(k, v));
	}
	else {
		putGetHelloWorld->put(k + "_" + putGetHelloWorld->getCompression(), v + "_" + putGetHelloWorld->getCompression(), putGetHelloWorld->getPutOpt());
	}

	ASSERT_NOT_NULL(putGetHelloWorld->getWaitOptions());
	EXPECT_EQ(v, putGetHelloWorld->waitFor(k, putGetHelloWorld->getWaitOpt()));
}

int main(int argc, char ** argv) {
	::testing::InitGoogleTest(&argc, argv);	// important that this is before parseCmdLine because there are some gtest options that mess up parseCmdLine, i.e. those starting with "--"
	CmdLineOptions o = Util::parseCmdLine(argc, argv);
	putGetHelloWorld = new PutGetWait(o.gcName, o.host, o.ns, o.logfile, o.verbose, o.nsOptions, o.jvmOptions, o.compressType, o.valueVersion, o.retrievalType, o.key, o.value, o.timeout, o.threshold);

	return RUN_ALL_TESTS(); 
}