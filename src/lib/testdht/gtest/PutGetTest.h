#ifndef PUT_GET_TEST
#define PUT_GET_TEST

#include "PutGet.h"
#include "gtest/gtest.h"

class PutGetTest : public ::testing::Test {
	public:
		 PutGetTest();
		~PutGetTest();

	protected:
		virtual void setUp() {}
		virtual void tearDown() {}

};

PutGet* putGetHelloWorld;

#endif
