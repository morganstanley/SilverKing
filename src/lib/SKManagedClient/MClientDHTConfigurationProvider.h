#pragma once

using namespace System;

namespace SKManagedClient {

	ref class MClientDHTConfiguration;

	ref class SKClientDHTConfigurationProvider_M 
	{
	public:
		void * pProvider; //(SKClientDHTConfigurationProvider*)//
	};

	public ref class MClientDHTConfigurationProvider abstract
	{
	public:
	    virtual MClientDHTConfiguration ^ getClientDHTConfiguration() =0;

	//internal:
	//	virtual SKClientDHTConfigurationProvider_M ^ getPImpl() =0;
	};
}
