#pragma once
#include "MSyncReadableNSPerspective.h"
#include "MSyncWritableNSPerspective.h"

using namespace System;
using namespace System::Collections;
using namespace System::Collections::Generic;

namespace SKManagedClient {

	ref class MNamespace;
	ref class MNamespacePerspectiveOptions;
	ref class MVersionConstraint;
	ref class MVersionProvider;
	interface class MRetrievalOptions;
	ref class MGetOptions;
	ref class MWaitOptions;
	ref class MPutOptions;
	ref class MStoredValue;

	ref class SKSyncNSPerspective_M 
	{
	public:
		void * pSnsp; // (SKSyncNSPerspective*) //
	};

	public ref class MSyncNSPerspective : public MSyncReadableNSPerspective, public MSyncWritableNSPerspective
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

		virtual Dictionary<String ^ , String ^ > ^ get(HashSet<String ^> ^ dhtKeys);
		virtual Dictionary<String ^ , MStoredValue ^ > ^ get(HashSet<String ^> ^ dhtKeys, MGetOptions ^ getOptions);
		virtual MStoredValue ^ get(String ^ key, MGetOptions ^ getOptions);
		virtual String ^ get(String ^ key);
		virtual Dictionary<String ^ , String ^ > ^ waitFor(HashSet<String ^ > ^ dhtKeys);
		virtual Dictionary<String ^ , MStoredValue ^ > ^ waitFor(HashSet<String ^ > ^ dhtKeys, MWaitOptions ^ waitOptions);
		virtual MStoredValue ^ waitFor(String ^  key, MWaitOptions ^ waitOptions);
		virtual String ^ waitFor(String ^ key);

		virtual void put(Dictionary<String ^ , String ^ > ^ dhtValues);
		virtual void put(Dictionary<String ^ , String ^ > ^ dhtValues, MPutOptions ^ putOptions);
		virtual void put(String ^ key, String ^ value, MPutOptions ^ putOptions);
		virtual void put(String ^ key, String ^ value);
		virtual void snapshot();
		virtual void snapshot(Int64 version);
		virtual void syncRequest();
		virtual void syncRequest(Int64 version);

		!MSyncNSPerspective();
		virtual ~MSyncNSPerspective();

	internal:
		MSyncNSPerspective(SKSyncNSPerspective_M ^ ansp);
		SKSyncNSPerspective_M ^ getPImpl();

	protected:
		Dictionary<String ^ , String ^ > ^ retrieve(HashSet<String ^> ^ key, bool isWaitFor);
		String ^ retrieve(String ^ key, bool isWaitFor);
		Dictionary<String ^ , MStoredValue ^ > ^ retrieve(HashSet<String ^ > ^ dhtKeys, MRetrievalOptions ^ retrievalOptions, bool isWaitFor);
		MStoredValue ^ retrieve(String ^ key, MRetrievalOptions ^ retrievalOptions, bool isWaitFor);

		void put_(Dictionary<String ^ , String ^ > ^ dhtValues, MPutOptions ^ putOptions, bool hasOptions);
		void put_(String ^ key, String ^ value, MPutOptions ^ putOptions, bool hasOptions);

	protected:
		void * pImpl;
	};


}
