#ifndef MULTI_PUT_MULTI_GET_TEST_H
#define MULTI_PUT_MULTI_GET_TEST_H

#include "MultiPutMultiGet.h"

#include "gtest/gtest.h"

class MultiPutMultiGetTest : public ::testing::Test {
	protected:
		virtual void setUp() {}
		virtual void tearDown() {}

	public:

};

MultiPutMultiGet* putGetHelloWorld;

#endif
