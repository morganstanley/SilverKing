#include "SKManagedClient.h"
#include "MAsyncKeyedOperation.h"

using namespace System;

namespace SKManagedClient {

	public ref class MAsyncSnapshot : public MAsyncOperation
	{
	public:
		virtual ~MAsyncSnapshot(void);
		!MAsyncSnapshot(void);

	internal:
		MAsyncSnapshot(SKAsyncOperation_M ^ asyncSnapshot);
		virtual SKAsyncOperation_M ^ getPImpl() override;

	private:
		void * pImpl;

	};



}
