#pragma once

using namespace System;

namespace SKManagedClient {

	ref class SKValueCreator_M
	{
	public:
		void * pValueCreator;  // (SKValueCreator*) //
	};

	public ref class MValueCreator
	{
	public:
		static const int BYTES = 8;  //IPV4_BYTES + BYTES_PER_INT

		~MValueCreator();
		!MValueCreator();

		String ^ getIP() ;  
		Int32 getID() ;
		//String ^ getBytes() const ;
		array<Byte> ^ getBytes();

	internal:
		MValueCreator(SKValueCreator_M ^ valueCreator);
		SKValueCreator_M ^ getPImpl();

	private:
		void * pImpl;
	};



}
