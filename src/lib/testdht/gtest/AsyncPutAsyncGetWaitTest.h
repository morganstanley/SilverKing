#ifndef ASYNC_PUT_ASYNC_GET_WAIT_TEST_H
#define ASYNC_PUT_ASYNC_GET_WAIT_TEST_H

#include "AsyncPutAsyncGetWait.h"

#include "gtest/gtest.h"

class AsyncPutAsyncGetWaitTest : public ::testing::Test {
	public:
		 AsyncPutAsyncGetWaitTest();
		~AsyncPutAsyncGetWaitTest();

	protected:
		virtual void setUp() {}
		virtual void tearDown() {}
};

AsyncPutAsyncGetWait* putGetHelloWorld;

#endif
