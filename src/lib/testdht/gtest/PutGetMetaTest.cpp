#include "PutGetMetaTest.h"

#include "TestUtil.h"

PutGetMetaTest:: PutGetMetaTest() {}
PutGetMetaTest::~PutGetMetaTest() { delete putGetHelloWorld; }

TEST_F(PutGetMetaTest, PutGetHelloWorld) {
	TestUtil::initAndTest(putGetHelloWorld);

	ASSERT_NOT_NULL(putGetHelloWorld->getSyncNSPerspective());
	ASSERT_NOT_NULL(putGetHelloWorld->getMetaGetOptions());

	string k = putGetHelloWorld->getKey();
	string v = putGetHelloWorld->getValue();
	TestUtil::checkKeyAndValue(k, v);
	
	if (!putGetHelloWorld->isCompressSet()) {
		ASSERT_NO_THROW(putGetHelloWorld->put(k, v));
	}
	else {
		putGetHelloWorld->put(k + "_" + putGetHelloWorld->getCompression(), v + "_" + putGetHelloWorld->getCompression(), putGetHelloWorld->getPutOpt());
	}
	
	ASSERT_NOT_NULL(putGetHelloWorld->getGetOpt());
	EXPECT_FALSE(putGetHelloWorld->getMeta(k, putGetHelloWorld->getGetOpt()).empty());
}

int main(int argc, char ** argv) {
	::testing::InitGoogleTest(&argc, argv);	// important that this is before parseCmdLine because there are some gtest options that mess up parseCmdLine, i.e. those starting with "--"
	CmdLineOptions o = Util::parseCmdLineMeta(argc, argv);
	putGetHelloWorld = new PutGetMeta(o.gcName, o.host, o.ns, o.logfile, o.verbose, o.nsOptions, o.jvmOptions, o.compressType, o.valueVersion, o.retrievalType, o.key, o.value);

	return RUN_ALL_TESTS(); 
}
