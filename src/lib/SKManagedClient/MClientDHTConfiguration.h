#pragma once

#include "MClientDHTConfigurationProvider.h"

using namespace System;
using namespace System::Collections::Generic;

namespace SKManagedClient {

	ref class SKClientDHTConfiguration_M 
	{
	public:
		void * pDhtConfig;
	};

	public ref class MClientDHTConfiguration : public MClientDHTConfigurationProvider
	{
	public:
		static MClientDHTConfiguration ^ create(Dictionary<String^,String^> ^ envMap);
		!MClientDHTConfiguration();
		virtual ~MClientDHTConfiguration() { this->!MClientDHTConfiguration(); };
		MClientDHTConfiguration(String ^ dhtName, String ^ zkLocs);
		MClientDHTConfiguration(String ^ dhtName, int dhtPort, String ^ zkLocs);

		String ^ getName();
		int getPort();
		String ^ toString();
		bool hasPort();
		//String ^ getZkLocs();   //should be deallocated with delete[]
		virtual MClientDHTConfiguration ^ getClientDHTConfiguration() override;

	internal:
		MClientDHTConfiguration(SKClientDHTConfiguration_M ^ pClientDHTConfiguration);
		SKClientDHTConfiguration_M ^ getPImpl();
	private:
		void * pImpl;
	};


}
