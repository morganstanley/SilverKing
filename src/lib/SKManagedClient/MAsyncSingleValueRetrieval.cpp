#include "StdAfx.h"
#include "MAsyncSingleValueRetrieval.h"
#include "MStoredValue.h"

#include <string>
using namespace std;
#include "skconstants.h"
#include "skcontainers.h"
#include "skbasictypes.h"
#include "SKAsyncSingleValueRetrieval.h"
#include "SKStoredValue.h"


namespace SKManagedClient {


MAsyncSingleValueRetrieval::~MAsyncSingleValueRetrieval(void)
{
	this->!MAsyncSingleValueRetrieval();
}
MAsyncSingleValueRetrieval::!MAsyncSingleValueRetrieval(void)
{
	if(pImpl)
	{
		delete (SKAsyncSingleValueRetrieval*)pImpl ; 
		pImpl = NULL;
	}
}
MAsyncSingleValueRetrieval::MAsyncSingleValueRetrieval(SKAsyncOperation_M ^ retrieval)
{
	pImpl = retrieval->pAsyncOperation;
}
SKAsyncOperation_M ^ MAsyncSingleValueRetrieval::getPImpl()
{
	SKAsyncOperation_M ^ retrieval = gcnew SKAsyncOperation_M;
	retrieval->pAsyncOperation = pImpl;
	return retrieval;
}

String ^ MAsyncSingleValueRetrieval::getValue()
{
	SKVal * val =  ((SKAsyncSingleValueRetrieval*)pImpl)->getValue();
	String ^ str = gcnew String( (char*)(val->m_pVal), 0, val->m_len );
	sk_destroy_val( &val );
	return str;
}

//MAsyncSingleRetrieval
MStoredValue ^ MAsyncSingleValueRetrieval::getStoredValue()
{
	SKStoredValue * pval =  ((SKAsyncSingleValueRetrieval*)pImpl)->getStoredValue();
	SKStoredValue_M ^ val = gcnew SKStoredValue_M;
	val->pStoredValue = pval;
	MStoredValue ^ storedValue = gcnew MStoredValue(val);
	return storedValue;
}


}

