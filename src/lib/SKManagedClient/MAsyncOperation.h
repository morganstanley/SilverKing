#pragma once

#include "SKManagedClient.h"

using namespace System;

namespace SKManagedClient {

	ref class SKAsyncOperation_M 
	{
	internal:
		void * pAsyncOperation; // subclasses of (SKAsyncOperation*) //
	};

	public ref class MAsyncOperation abstract
	{
	public:
		/**
		* Query operation state.
		* @return operation state
		*/
		virtual SKOperationState_M getState();

		/**
		* Query cause of failure. Only valid for operations that have, in fact, failed.
		* @return underlying FailureCause
		*/
		virtual SKFailureCause_M getFailureCause();

		/**
		* Block until this operation is complete.
		*/
		virtual void waitForCompletion(); // throws MOperationException;

		/**
		* Block until this operation is complete. Exit after the given timeout
		* @param timeout time to wait
		* @param unit unit of time to wait
		* @return true if this operation is complete. false otherwise
		* @throws OperationException
		*/
		virtual bool waitForCompletion(long timeout, SKTimeUnit_M unit); //throws OperationException;

		/**
		* Close this asynchronous operation. No subsequent calls may be issued against
		* this reference.
		*/
		virtual void close();
		virtual ~MAsyncOperation();
		!MAsyncOperation();
	internal:
		virtual SKAsyncOperation_M ^ getPImpl() = 0;
	protected:
		void * pImpl;
		MAsyncOperation();

	};

}