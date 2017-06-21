#ifndef NAMESPACE_HANDLING_TEST_H
#define NAMESPACE_HANDLING_TEST_H

#include "NamespaceHandling.h"

#include "gtest/gtest.h"

class NamespaceHandlingTest : public ::testing::Test {
	public:
		 NamespaceHandlingTest();
		~NamespaceHandlingTest();

	protected:
		virtual void setUp() {}
		virtual void tearDown() {}
};

NamespaceHandling* namespaceHandling;

#endif
