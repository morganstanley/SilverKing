#ifndef PUT_GET_WAIT_TEST_H
#define PUT_GET_WAIT_TEST_H

#include "PutGetWait.h"

#include "gtest/gtest.h"

class PutGetWaitTest : public ::testing::Test {
	public:
		 PutGetWaitTest();
		~PutGetWaitTest();

	protected:
		virtual void setUp() {}
		virtual void tearDown() {}

};

PutGetWait* putGetHelloWorld;

#endif
