#ifndef ASYNC_PUT_ASYNC_GET_META_TEST_H
#define ASYNC_PUT_ASYNC_GET_META_TEST_H

#include "AsyncPutAsyncGetMeta.h"

#include "gtest/gtest.h"

class AsyncPutAsyncGetMetaTest : public ::testing::Test {
	public:
		 AsyncPutAsyncGetMetaTest();
		~AsyncPutAsyncGetMetaTest();

	protected:
		virtual void setUp() {}
		virtual void tearDown() {}

};

AsyncPutAsyncGetMeta* putGetHelloWorld;

#endif
