#pragma once

#include "SKManagedClient.h"

using namespace System;

namespace SKManagedClient {

	ref class SKSecondaryTarget_M
	{
	public:
		void * pSecondaryTarget;  // (SKSecondaryTarget*) //
	};

	public ref class MSecondaryTarget
	{
	public:
		~MSecondaryTarget();
		!MSecondaryTarget();
		MSecondaryTarget(SKSecondaryTargetType_M type, String ^ target);
		SKSecondaryTargetType_M getType();
		String ^ getTarget();
	internal:
		MSecondaryTarget(SKSecondaryTarget_M ^ secondaryTarget);
		SKSecondaryTarget_M ^ getPImpl();

	private:
		void * pImpl;
	};



}
