#pragma once

#include "MBaseNSPerspective.h"
using namespace System;
using namespace System::Collections;
using namespace System::Collections::Generic;

namespace SKManagedClient {

	ref class MPutOptions;

	public interface class MSyncWritableNSPerspective : public MBaseNSPerspective
	{
	public:
		virtual void put(Dictionary<String ^ , String ^ > ^ dhtValues);
		virtual void put(Dictionary<String ^ , String ^ > ^ dhtValues, MPutOptions ^ putOptions);
		virtual void put(String ^ key, String ^ value, MPutOptions ^ putOptions);
		virtual void put(String ^ key, String ^ value);
		virtual void snapshot();
		virtual void snapshot(Int64 version);
		virtual void syncRequest();
		virtual void syncRequest(Int64 version);

	};

}

