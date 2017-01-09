#include "StdAfx.h"
#include "MStoredValue.h"

#include <string>
using namespace std;

#include "skbasictypes.h"
#include "SKMetaData.h"
#include "SKStoredValue.h"


namespace SKManagedClient {



MStoredValue::~MStoredValue(void)
{
	this->!MStoredValue();
}

MStoredValue::!MStoredValue(void)
{
	if(pImpl)
	{
		delete (SKStoredValue*)pImpl; 
		pImpl = NULL;
	}
}

MStoredValue::MStoredValue(SKStoredValue_M ^ storedValue)
{
	pImpl = storedValue->pStoredValue;
}

/*
SKMetaData_M ^ MStoredValue::getPImpl()
{
	SKMetaData_M ^ storedValue = gcnew SKMetaData_M;
	storedValue->pStoredValue = pImpl;
	return storedValue;
}
*/

MMetaData ^ MStoredValue::getMetaData() 
{
	SKMetaData * pMetaData = ((SKStoredValue*)pImpl)->getMetaData();
	SKMetaData_M ^ metaData = gcnew SKMetaData_M;
	metaData->pMetaData  = pMetaData;
	MMetaData ^ md = gcnew MMetaData(metaData);
	return md;
}

String ^ MStoredValue::getValue() 
{
	SKVal * val =  ((SKStoredValue*)pImpl)->getValue();
	String ^ str = gcnew String( (char*)(val->m_pVal), 0, val->m_len );
	sk_destroy_val( &val );
	return str;
}

MStoredValue ^ MStoredValue::next() 
{
	SKStoredValue * val =  ((SKStoredValue*)pImpl)->next();
	SKStoredValue_M ^ storedValue = gcnew SKStoredValue_M;
	storedValue->pStoredValue = val;
	MStoredValue ^ sv = gcnew MStoredValue(storedValue);
	return sv;
}


}

