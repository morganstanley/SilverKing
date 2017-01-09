#pragma once

#include "SKManagedClient.h"
#include "MAsyncKeyedOperation.h"
#include "MAsyncOperation.h"

using namespace System;
using namespace System::Collections;
using namespace System::Collections::Generic;

namespace SKManagedClient {

	ref class MStoredValue;
	///ref class SKAsyncOperation_M;

	public ref class MAsyncRetrieval : public MAsyncKeyedOperation
	{
	public:

		/**
		* Returns StoredValues for all successfully complete retrievals that
		* have completed since the last call to this method or AsyncValueRetrieval.getLatestStoredValues(). 
		* Each successfully retrieved value will be reported exactly once by this 
		* method and AsyncValueRetrieval.getLatestStoredValues().
		* This method is unaffected by calls to getStoredValues(). 
		* Concurrent execution is permitted, but the precise values returned are
		* undefined.  
		*/
		virtual Dictionary<String ^ , MStoredValue ^ > ^  getLatestStoredValues();  //throws RetrievalException

		/**
		* Returns StoredValues for all successfully complete retrievals. 
		* @return StoredValues for all successfully complete retrievals
		* @throws RetrievalException
		*/
		virtual Dictionary<String ^ , MStoredValue ^ > ^  getStoredValues();  //throws RetrievalException

		/**
		* Returns StoredValues for the given key if it is present. 
		* @param key key to query
		* @return StoredValues for the given key if it is present
		* @throws RetrievalException
		*/
		virtual MStoredValue ^  getStoredValue(String ^ key);   //throws RetrievalException


		virtual ~MAsyncRetrieval();
		!MAsyncRetrieval();

	internal:
		MAsyncRetrieval(SKAsyncOperation_M ^ asyncRetrieval);
		virtual SKAsyncOperation_M ^ getPImpl() override;
	protected:
		MAsyncRetrieval();
		Dictionary<String ^ , MStoredValue ^ > ^  retrieveStoredValues(bool latest);
		

	};

}