#pragma once
#include "MClientDHTConfigurationProvider.h"

using namespace System;
using namespace System::Collections;
using namespace System::Collections::Generic;

namespace SKManagedClient {

	ref class MClientDHTConfiguration;

	ref class SKGridConfiguration_M 
	{
	public: 
		void * pGridConfig; // (SKGridConfiguration*) //
	};

	public ref class MGridConfiguration : public MClientDHTConfigurationProvider
	{
	public:
		static MGridConfiguration ^ parseFile(System::String ^ gcBase, System::String ^ gcName); //1st arg was File
		static MGridConfiguration ^ parseFile(System::String ^ gcName);
		static Dictionary<String ^ , String ^ > ^ readEnvFile(System::String ^ envFile);  //the arg was File

		MGridConfiguration(System::String ^ name, Dictionary<String ^ , String ^ > ^ envMap); 
		System::String ^ getName();      
		System::String ^ get(System::String ^ envKey);
		System::String ^ toString() ; 
		Dictionary<String ^ , String ^ > ^ getEnvMap();
		virtual MClientDHTConfiguration ^ getClientDHTConfiguration() override;

		!MGridConfiguration();
		~MGridConfiguration();

	internal:
		virtual SKGridConfiguration_M ^ getPImpl();  //pImpl-related accessor
		MGridConfiguration(SKGridConfiguration_M ^ gridConfImpl);

	private:
		void * pImpl;

	};

}
