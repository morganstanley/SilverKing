#ifndef PUT_GET_META_TEST_H
#define PUT_GET_META_TEST_H

#include "PutGetMeta.h"

#include "gtest/gtest.h"

class PutGetMetaTest : public ::testing::Test {
	public:
		 PutGetMetaTest();
		~PutGetMetaTest();

	protected:
		virtual void setUp() {}
		virtual void tearDown() {}

};

PutGetMeta* putGetHelloWorld;

#endif
