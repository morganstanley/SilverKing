#pragma once
#include "MVersionProvider.h"

using namespace System;

namespace SKManagedClient {

	ref class MAbsMillisTimeSource;

	ref class SKAbsMillisVersionProvider_M 
	{
	public: 
		void * pVersionProvider; // (SKAbsMillisVersionProvider*) //
	};

	public ref class MAbsMillisVersionProvider : public MVersionProvider
	{
	public:
		MAbsMillisVersionProvider(MAbsMillisTimeSource ^ absMillisTimeSource);
		virtual ~MAbsMillisVersionProvider();
		!MAbsMillisVersionProvider();
	//	virtual Int64 getVersion();

	internal:
		MAbsMillisVersionProvider(SKAbsMillisVersionProvider_M ^ versionProviderImpl);

	};

}