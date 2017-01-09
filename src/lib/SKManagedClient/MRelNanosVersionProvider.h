#pragma once
#include "MVersionProvider.h"
#include "MRelNanosTimeSource.h"

using namespace System;

namespace SKManagedClient {

	ref class MRelNanosAbsMillisTimeSource;

	ref class SKRelNanosVersionProvider_M 
	{
	internal:
		void * pVersionProvider; // (SKRelNanosVersionProvider*) //
	};

	public ref class MRelNanosVersionProvider : public MVersionProvider
	{
	public:
		virtual ~MRelNanosVersionProvider(void);
		!MRelNanosVersionProvider(void);
		MRelNanosVersionProvider(MRelNanosAbsMillisTimeSource ^ relNanosTimeSource);
		//virtual Int64 getVersion() override;

	internal:
		MRelNanosVersionProvider(SKRelNanosVersionProvider_M ^ versionProviderImpl);
		//virtual SKRelNanosVersionProvider_M ^ getPImpl();

	private:
		void * pImpl;
	};

}