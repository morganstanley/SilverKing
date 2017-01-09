#pragma once
#include "MAsyncReadableNSPerspective.h"
#include "MAsyncWritableNSPerspective.h"

using namespace System;
using namespace System::Collections;
using namespace System::Collections::Generic;

namespace SKManagedClient {

	ref class MNamespace;
	ref class MNamespacePerspectiveOptions;
	ref class MVersionConstraint;
	ref class MVersionProvider;
	ref class MAsyncValueRetrieval;
	ref class MAsyncRetrieval;
	ref class MAsyncSingleValueRetrieval;
	ref class MGetOptions;
	ref class MWaitOptions;
	ref class MPutOptions;
	interface class MRetrievalOptions;
	ref class MAsyncPut;
	ref class MAsyncSnapshot;
	ref class MAsyncSyncRequest;


	ref class SKAsyncNSPerspective_M 
	{
	public:
		void * pAnsp; // (SKAsyncNSPerspective*) //
	};

	public ref class MAsyncNSPerspective : public MAsyncReadableNSPerspective , public MAsyncWritableNSPerspective
	{
	public:

		virtual String ^ getName();
		virtual MNamespace ^ getNamespace();
		virtual MNamespacePerspectiveOptions ^ getOptions();
		virtual void setOptions(MNamespacePerspectiveOptions ^ nspOptions);
		virtual void setDefaultRetrievalVersionConstraint(MVersionConstraint ^ vc);
		virtual void setDefaultVersionProvider(MVersionProvider ^ versionProvider);
		virtual void setDefaultVersion(Int64 version);
		virtual void close();

		virtual MAsyncValueRetrieval ^ get(HashSet<String ^> ^ dhtKeys);
		virtual MAsyncRetrieval ^ get(HashSet<String ^> ^ dhtKeys, MGetOptions ^ getOptions);
		virtual MAsyncRetrieval ^ get(String ^ key, MGetOptions ^ getOptions);
		virtual MAsyncSingleValueRetrieval ^ get(String ^ key);
		virtual MAsyncValueRetrieval ^ waitFor(HashSet<String ^ > ^ dhtKeys);
		virtual MAsyncRetrieval ^ waitFor(HashSet<String ^ > ^ dhtKeys, MWaitOptions ^ waitOptions);
		virtual MAsyncRetrieval ^ waitFor(String ^  key, MWaitOptions ^ waitOptions);
		virtual MAsyncSingleValueRetrieval ^ waitFor(String ^ key);

		virtual MAsyncPut ^ put(Dictionary<String ^ , String ^ > ^ dhtValues);
		virtual MAsyncPut ^ put(Dictionary<String ^ , String ^ > ^ dhtValues, MPutOptions ^ putOptions);
		virtual MAsyncPut ^ put(String ^ key, String ^ value, MPutOptions ^ putOptions);
		virtual MAsyncPut ^ put(String ^ key, String ^ value);
		virtual MAsyncSnapshot ^ snapshot();
		virtual MAsyncSnapshot ^ snapshot(Int64 version);
		virtual MAsyncSyncRequest ^ syncRequest();
		virtual MAsyncSyncRequest ^ syncRequest(Int64 version);

		!MAsyncNSPerspective();
		virtual ~MAsyncNSPerspective();

	internal:
		MAsyncNSPerspective(SKAsyncNSPerspective_M ^ ansp);
		SKAsyncNSPerspective_M ^ getPImpl();

	protected:
		MAsyncValueRetrieval ^ retrieve(HashSet<String ^> ^ key, bool isWaitFor);
		MAsyncSingleValueRetrieval ^ retrieve(String ^ key, bool isWaitFor);
		MAsyncRetrieval ^ retrieve(HashSet<String ^ > ^ dhtKeys, MRetrievalOptions ^ retrievalOptions, bool isWaitFor);
		MAsyncRetrieval ^ retrieve(String ^ key, MRetrievalOptions ^ retrievalOptions, bool isWaitFor);

		MAsyncPut ^ put_(Dictionary<String ^ , String ^ > ^ dhtValues, MPutOptions ^ putOptions, bool hasOptions);
		MAsyncPut ^ put_(String ^ key, String ^ value, MPutOptions ^ putOptions, bool hasOptions);

		void * pImpl;
	};


}

