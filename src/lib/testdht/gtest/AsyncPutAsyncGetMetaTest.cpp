#include "AsyncPutAsyncGetMetaTest.h"
#include "TestUtil.h"

AsyncPutAsyncGetMetaTest:: AsyncPutAsyncGetMetaTest() {}
AsyncPutAsyncGetMetaTest::~AsyncPutAsyncGetMetaTest() { delete putGetHelloWorld; }

TEST_F(AsyncPutAsyncGetMetaTest, PutGetHelloWorld) {
	TestUtil::initAndTest(putGetHelloWorld);
  
	ASSERT_NOT_NULL(putGetHelloWorld->getAsyncNSPerspective());
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

	ASSERT_NOT_NULL(putGetHelloWorld->getMetaGetOpt());
	EXPECT_FALSE(putGetHelloWorld->getMeta(k, putGetHelloWorld->getMetaGetOpt()).empty());
}


int main(int argc, char ** argv) {
	::testing::InitGoogleTest(&argc, argv);	// important that this is before parseCmdLine because there are some gtest options that mess up parseCmdLine, i.e. those starting with "--"
	CmdLineOptions o = Util::parseCmdLineMeta(argc, argv);
	putGetHelloWorld = new AsyncPutAsyncGetMeta(o.gcName, o.host, o.ns, o.logfile, o.verbose, o.nsOptions, o.jvmOptions, o.compressType, o.valueVersion, o.retrievalType, o.key, o.value, o.timeout);

	return RUN_ALL_TESTS(); 
}

