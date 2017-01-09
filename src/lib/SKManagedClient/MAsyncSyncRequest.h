#pragma once

#include "SKManagedClient.h"
#include "MAsyncOperation.h"

using namespace System;

namespace SKManagedClient {

	public ref class MAsyncSyncRequest : public MAsyncOperation
	{
	public:
		virtual ~MAsyncSyncRequest(void);
		!MAsyncSyncRequest(void);

	internal:
		MAsyncSyncRequest(SKAsyncOperation_M ^ asyncSyncRequest);
		virtual SKAsyncOperation_M ^ getPImpl() override;

	};


}
