#pragma once
#include "MAbsMillisTimeSource.h"

using namespace System;

namespace SKManagedClient {

	ref class SKConstantAbsMillisTimeSource_M 
	{
	internal:
		void * pTimeSource; // (SKConstantAbsMillisTimeSource*) //
	};


	public ref class MConstantAbsMillisTimeSource : public MAbsMillisTimeSource
	{
	public:
		!MConstantAbsMillisTimeSource();
		virtual ~MConstantAbsMillisTimeSource();
		MConstantAbsMillisTimeSource(Int64 absMillisTime);

	internal:
		MConstantAbsMillisTimeSource(SKConstantAbsMillisTimeSource_M ^ timeSource);
		SKConstantAbsMillisTimeSource_M ^ getPImpl();

	};

}