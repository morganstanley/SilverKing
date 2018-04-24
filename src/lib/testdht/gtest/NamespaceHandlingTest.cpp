#include "NamespaceHandlingTest.h"

#include "TestUtil.h"

NamespaceHandlingTest:: NamespaceHandlingTest() {}
NamespaceHandlingTest::~NamespaceHandlingTest() { delete namespaceHandling; }

TEST_F(NamespaceHandlingTest, Clone) {
	TestUtil::initAndTest(namespaceHandling);
	
	ASSERT_NO_THROW(namespaceHandling->cloneNamespace(namespaceHandling->getSession(), namespaceHandling->getNamespaceName()));
	ASSERT_NOT_NULL(namespaceHandling->getChildNamespace());
  
	SKNamespaceOptions* nsOpt = namespaceHandling->getNamespaceOptions();
	SKConsistency oldConsistencyProtocol        = nsOpt->getConsistencyProtocol(); 
	SKStorageType::SKStorageType oldStorageType = nsOpt->getStorageType();
	SKVersionMode oldVersionMode                = nsOpt->getVersionMode();
	int oldSegmentSize                          = nsOpt->getSegmentSize(); 
	ASSERT_NO_THROW(namespaceHandling->setNamespaceOptions(nsOpt, SKStorageType::RAM, SKConsistency::LOOSE, SKVersionMode::SEQUENTIAL, 33554432));

	// set back
	ASSERT_NO_THROW(namespaceHandling->setNamespaceOptions(nsOpt, oldStorageType, oldConsistencyProtocol, oldVersionMode, oldSegmentSize));
}

int main(int argc, char ** argv) {
	::testing::InitGoogleTest(&argc, argv);	// important that this is before parseCmdLine because there are some gtest options that mess up parseCmdLine, i.e. those starting with "--"
	CmdLineOptions o = Util::parseCmdLine(argc, argv);
	namespaceHandling = new NamespaceHandling(o.gcName, o.host, o.ns, o.logfile, o.verbose, o.nsOptions, o.jvmOptions, o.compressType, o.valueVersion, o.retrievalType);

	return RUN_ALL_TESTS(); 
}