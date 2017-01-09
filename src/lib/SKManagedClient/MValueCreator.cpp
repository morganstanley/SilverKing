#include "StdAfx.h"
#include "MValueCreator.h"

#include <cstddef>
#include "skbasictypes.h"
#include "SKValueCreator.h"

using namespace System;
using namespace System::Net;
using namespace System::Runtime::InteropServices;

namespace SKManagedClient {

MValueCreator::~MValueCreator()
{
	this->!MValueCreator();
}

MValueCreator::!MValueCreator()
{
	if(pImpl)
	{
		delete (SKValueCreator*)pImpl ;
		pImpl = NULL;
	}
}

MValueCreator::MValueCreator(SKValueCreator_M ^ valueCreator)
{
	pImpl = valueCreator->pValueCreator;
}
SKValueCreator_M ^ MValueCreator::getPImpl()
{
	SKValueCreator_M ^ valueCreator = gcnew SKValueCreator_M;
	valueCreator->pValueCreator = pImpl;
	return valueCreator;
}

String ^ MValueCreator::getIP() 
{
	SKVal * pVal = ((SKValueCreator*)pImpl)->getIP();
	int *   pInt = reinterpret_cast<int*>(pVal->m_pVal);
	IPAddress ^ ip = gcnew IPAddress(BitConverter::GetBytes(*pInt));
	String ^ str = ip->ToString();
	sk_destroy_val(&pVal);
	return str;
}

Int32 MValueCreator::getID() 
{
	return ((SKValueCreator*)pImpl)->getID();
}

/*  // Alternative impl with byte array return
String ^ MValueCreator::getBytes() const 	
{
	SKVal * pVal = ((SKValueCreator*)pImpl)->getBytes();
	System::String ^ str = gcnew System::String((char*)(pVal->m_pVal), 0, pVal->m_len);
	sk_destroy_val(&pVal);
	return str;
}
*/

array<Byte> ^ MValueCreator::getBytes() 	
{
	SKVal * pVal = ((SKValueCreator*)pImpl)->getBytes();
	array<Byte>^ bytes = gcnew array<Byte>(pVal->m_len);
	Marshal::Copy(IntPtr( (char *) pVal->m_pVal ), bytes, 0, bytes->Length);
	sk_destroy_val(&pVal);
	return bytes;
}

}