#include "StdAfx.h"
#include "MAsyncValueRetrieval.h"

#include <string>
using namespace std;
#include "skconstants.h"
#include "skcontainers.h"
#include "skbasictypes.h"
#include "SKAsyncValueRetrieval.h"

using namespace System::Runtime::InteropServices;


namespace SKManagedClient {


MAsyncValueRetrieval::~MAsyncValueRetrieval(void)
{
	this->!MAsyncValueRetrieval();
}

MAsyncValueRetrieval::!MAsyncValueRetrieval(void)
{
	if(pImpl) 
	{
		delete (SKAsyncValueRetrieval*)pImpl ; 
		pImpl = NULL;
	}
}

MAsyncValueRetrieval::MAsyncValueRetrieval(SKAsyncOperation_M ^ asyncValueRetrieval)
{
	pImpl = asyncValueRetrieval->pAsyncOperation;
}

SKAsyncOperation_M ^ MAsyncValueRetrieval::getPImpl()
{
	SKAsyncOperation_M ^ asyncValueRetrieval = gcnew SKAsyncOperation_M;
	asyncValueRetrieval->pAsyncOperation = pImpl;
	return asyncValueRetrieval;
}


//protected
MAsyncValueRetrieval::MAsyncValueRetrieval() 
{ 
	//
}

Dictionary<String ^ , String ^ > ^ MAsyncValueRetrieval::retrieveValues(bool latest)
{
	StrValMap * vals = NULL; 
	if(latest)
	{
		vals = ((SKAsyncValueRetrieval*)pImpl)->getLatestValues();
	}
	else
	{
		vals = ((SKAsyncValueRetrieval*)pImpl)->getValues();
	}
	
	int nKeys = vals->size();
	Dictionary<String^, String^>^ values = gcnew Dictionary<String^, String^>(nKeys);

	for (StrValMap::iterator it=vals->begin(); it!=vals->end(); vals++) {
		String ^ skey = gcnew String(it->first.c_str());
		SKVal * pval = it->second;
        if( pval != NULL ){
			String ^ sval = gcnew String( (char*)pval->m_pVal, 0, pval->m_len );
			values->Add(skey, sval);
		    sk_destroy_val( &pval );
        }else  {
			values->Add(skey, nullptr);
        }
	}
	delete vals ; vals = NULL ;
	return values;
}

//MAsyncValueRetrieval
Dictionary<String ^ , String ^ > ^ MAsyncValueRetrieval::getLatestValues()
{
	return this->retrieveValues(true);
}

Dictionary<String ^ , String ^ > ^ MAsyncValueRetrieval::getValues()
{
	return this->retrieveValues(false);
}

String ^ MAsyncValueRetrieval::getValue(String ^ key)
{
	String ^ str = nullptr;
	char* key_c =  NULL;
	try { 
		key_c = (char*)(void*)Marshal::StringToHGlobalAnsi(key);
		string strKey (key_c);
		SKVal * val =  ((SKAsyncValueRetrieval*)pImpl)->getValue(&strKey);
		str = gcnew String( (char*)(val->m_pVal), 0, val->m_len );
		sk_destroy_val( &val );
	}
	finally {
		if (key_c) Marshal::FreeHGlobal(System::IntPtr(key_c));
	}

	return str;
}


}

