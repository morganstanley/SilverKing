#ifndef ASYNC_PUT_ASYNC_GET_TEST_H
#define ASYNC_PUT_ASYNC_GET_TEST_H

#include "AsyncPutAsyncGet.h"
#include "gtest/gtest.h"

class AsyncPutAsyncGetTest : public ::testing::Test {

	public:
		 AsyncPutAsyncGetTest();
		~AsyncPutAsyncGetTest();

	protected:
		virtual void setUp() {}
		virtual void tearDown() {}

};

AsyncPutAsyncGet* putGetHelloWorld;

#endif
