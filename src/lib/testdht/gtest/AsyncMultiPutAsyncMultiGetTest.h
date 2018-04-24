#ifndef ASYNC_MULTI_PUT_ASYNC_MULTI_GET_TEST_H
#define ASYNC_MULTI_PUT_ASYNC_MULTI_GET_TEST_H

#include "AsyncMultiPutAsyncMultiGet.h"
#include "gtest/gtest.h"

class AsyncMultiPutAsyncMultiGetTest : public ::testing::Test {
	protected:
		virtual void setUp() {}
		virtual void tearDown() {}

	public:

};

AsyncMultiPutAsyncMultiGet* putGetHelloWorld;

#endif
