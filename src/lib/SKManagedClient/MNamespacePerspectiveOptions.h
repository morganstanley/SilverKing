#pragma once
#include "SKManagedClient.h"

using namespace System;

namespace SKManagedClient {

	ref class MVersionProvider;
	ref class MPutOptions;
	ref class MGetOptions;
	ref class MWaitOptions;

	ref class SKNamespacePerspectiveOptions_M
	{
	public:
		void * pNspOptions; // SKNamespacePerspectiveOptions *
	};

	public ref class MNamespacePerspectiveOptions
	{
	public:
		//Class<K> getKeyClass();
		//Class<V> getValueClass();

		SKKeyDigestType_M getKeyDigestType();
		MPutOptions ^ getDefaultPutOptions();
		MGetOptions ^ getDefaultGetOptions();
		MWaitOptions ^ getDefaultWaitOptions();
		MVersionProvider ^ getDefaultVersionProvider();

		MNamespacePerspectiveOptions ^ keyDigestType(SKKeyDigestType_M keyDigestType);
		MNamespacePerspectiveOptions ^ defaultPutOptions(MPutOptions ^ defaultPutOptions);
		MNamespacePerspectiveOptions ^ defaultGetOptions(MGetOptions ^ defaultGetOptions);
		MNamespacePerspectiveOptions ^ defaultWaitOptions(MWaitOptions ^ defaultWaitOptions);
		MNamespacePerspectiveOptions ^ defaultVersionProvider(MVersionProvider ^ defaultVersionProvider);

		MNamespacePerspectiveOptions ^ parse(System::String ^ def); 
		String ^ toString();

		!MNamespacePerspectiveOptions();
		~MNamespacePerspectiveOptions();
		MNamespacePerspectiveOptions(/*KeyClass k, ValueClass v,*/ SKKeyDigestType_M keyDigestType, 
										MPutOptions ^ defaultPutOptions, MGetOptions ^ defaultGetOptions, 
										MWaitOptions ^ defaultWaitOptions, MVersionProvider ^ defaultVersionProvider);

	internal:
		MNamespacePerspectiveOptions(SKNamespacePerspectiveOptions_M ^  pNspOpt);
		SKNamespacePerspectiveOptions_M ^ getPImpl();
	private:
		void * pImpl;

	};

}