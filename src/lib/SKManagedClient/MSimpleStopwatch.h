#pragma once

#include "MStopwatchBase.h"

using namespace System;

namespace SKManagedClient {

	ref class MRelNanosAbsMillisTimeSource;

	ref class SKSimpleStopwatch_M
	{
		internal:
			void * pStopwatch; // (SKSimpleStopwatch*) //
	};
	
	public ref class MSimpleStopwatch : public MStopwatchBase
	{
	public:

		MSimpleStopwatch();
		MSimpleStopwatch(MRelNanosAbsMillisTimeSource ^ relNanosTimeSource);
		!MSimpleStopwatch();
		virtual ~MSimpleStopwatch();

	internal:
		SKSimpleStopwatch_M ^ getPImpl();
		MSimpleStopwatch(SKSimpleStopwatch_M ^ stopwatch);

	private protected:
		MSimpleStopwatch(bool ignore);
	
	};

}