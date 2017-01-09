#pragma once
#include "SKManagedClient.h"

using namespace System;

namespace SKManagedClient {

	ref class MPutOptions;
	ref class MGetOptions;
	ref class MWaitOptions;

	ref class SKNamespaceOptions_M {
	internal:
		void * pNsOptions;
	};

	public ref class MNamespaceOptions
	{
	public:

		static MNamespaceOptions ^ parse(String ^ def);
		MNamespaceOptions(SKStorageType_M storageType, SKConsistency_M consistencyProtocol, 
							SKVersionMode_M versionMode, SKRevisionMode_M revisionMode, MPutOptions ^ defaultPutOptions,
							MGetOptions ^ defaultGetOptions, MWaitOptions ^ defaultWaitOptions,
							int secondarySyncIntervalSeconds, int segmentSize, bool allowLinks );
		MNamespaceOptions(SKStorageType_M storageType, SKConsistency_M consistencyProtocol, 
							SKVersionMode_M versionMode, SKRevisionMode_M revisionMode, MPutOptions ^ defaultPutOptions,
							MGetOptions ^ defaultGetOptions, MWaitOptions ^ defaultWaitOptions, 
							int secondarySyncIntervalSeconds, int segmentSize );
		MNamespaceOptions(SKStorageType_M storageType, SKConsistency_M consistencyProtocol, 
							SKVersionMode_M versionMode, MPutOptions ^ defaultPutOptions,
							MGetOptions ^ defaultGetOptions, MWaitOptions ^ defaultWaitOptions );

		SKConsistency_M getConsistencyProtocol();
		MPutOptions ^ getDefaultPutOptions();
		MGetOptions ^ getDefaultGetOptions();
		MWaitOptions ^ getDefaultWaitOptions();
		SKRevisionMode_M getRevisionMode();
		SKStorageType_M getStorageType();
		SKVersionMode_M getVersionMode();
		int getSegmentSize();
		bool getAllowLinks();
		int getSecondarySyncIntervalSeconds();
		String ^ toString();
		bool equals(MNamespaceOptions ^ other);

		MNamespaceOptions ^ segmentSize(int segmentSize);
		MNamespaceOptions ^ allowLinks(bool allowLinks);
		MNamespaceOptions ^ storageType(SKStorageType_M storageType);
		MNamespaceOptions ^ consistencyProtocol(SKConsistency_M consistencyProtocol);
		MNamespaceOptions ^ versionMode(SKVersionMode_M versionMode);
		MNamespaceOptions ^ revisionMode(SKRevisionMode_M revisionMode);
		MNamespaceOptions ^ defaultPutOptions(MPutOptions ^ defaultPutOptions);
		MNamespaceOptions ^ secondarySyncIntervalSeconds(int secondarySyncIntervalSeconds);
		!MNamespaceOptions();
		~MNamespaceOptions();

		//impl
	internal:
		MNamespaceOptions(SKNamespaceOptions_M ^ namespaceOptions);
		SKNamespaceOptions_M ^getPImpl();

	private:
		void * pImpl;
	};

}

