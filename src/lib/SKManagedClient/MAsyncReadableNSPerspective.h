#pragma once

#include "MBaseNSPerspective.h"
using namespace System;
using namespace System::Collections;
using namespace System::Collections::Generic;

namespace SKManagedClient {

	ref class MAsyncSyncRequest;
	ref class MAsyncValueRetrieval;
	ref class MAsyncRetrieval;
	ref class MAsyncSingleValueRetrieval;
	ref class MGetOptions;
	ref class MWaitOptions;


	public interface class MAsyncReadableNSPerspective : public MBaseNSPerspective
	{
	public:

		virtual MAsyncValueRetrieval ^ get(HashSet<String ^> ^ dhtKeys) ;
		virtual MAsyncRetrieval ^ get(HashSet<String ^> ^ dhtKeys, MGetOptions ^ getOptions);
		virtual MAsyncRetrieval ^ get(String ^ key, MGetOptions ^ getOptions);
		virtual MAsyncSingleValueRetrieval ^ get(String ^ key);
 
		virtual MAsyncValueRetrieval ^ waitFor(HashSet<String ^ > ^ dhtKeys);
		virtual MAsyncRetrieval ^ waitFor(HashSet<String ^ > ^ dhtKeys, MWaitOptions ^ waitOptions);
		virtual MAsyncRetrieval ^ waitFor(String ^  key, MWaitOptions ^ waitOptions);
		virtual MAsyncSingleValueRetrieval ^ waitFor(String ^ key);

	};
}