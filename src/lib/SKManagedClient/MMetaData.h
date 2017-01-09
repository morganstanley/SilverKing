#pragma once

#include "SKManagedClient.h"
#include "MValueCreator.h"

using namespace System;

namespace SKManagedClient {

	ref class SKMetaData_M 
	{
	internal:
		void * pMetaData ;  // (SKMetaData*)
	};

	public ref class MMetaData
	{
	public:
		virtual ~MMetaData(void);
		!MMetaData(void);

		/**
		* Length in bytes of the stored value. This length includes metadata.
		* @return
		*/
		Int32 getStoredLength();
		/**
		* Length in bytes of the actual value stored ignoring compression. 
		* @return
		*/
		Int32 getUncompressedLength();
		/**
		* Version of the value stored.
		* @return
		*/
		Int64 getVersion();
		/**
		* Time in milliseconds that a value was stored.
		* @return
		*/
		Int64 getCreationTime();
		/**
		* The ValueCreator responsible for storing the value.
		* @return
		*/
		MValueCreator ^ getCreator();
		/**
		* User data (possibly binary) associated with a value.
		* @return
		*/
		String ^ getUserData();
		/**
		* A string representation of this MetaData. 
		* @param labeled specifies whether or not to label each MetaData member
		* @return
		*/
		String ^ toString(bool labeled);
		/**
		* The stored checksum of this value.
		* @return
		*/
		String ^ getChecksum();
		/**
		* The Compression used to stored this value.
		* @return
		*/
		SKCompression_M getCompression() ;
		/**
		* The ChecksumType used to checksum this value.
		* @return
		*/
		SKChecksumType_M getChecksumType() ;

	internal:
		MMetaData(SKMetaData_M ^ metaData);
		SKMetaData_M ^ getPImpl();

	protected:
		MMetaData();
		void * pImpl;
	};



}

