#include "StdAfx.h"
#include "MAsyncRetrieval.h"
#include "MStoredValue.h"

#include <string>
using namespace std;
#include "skconstants.h"
#include "skcontainers.h"
#include "skbasictypes.h"
#include "SKAsyncRetrieval.h"
#include "SKStoredValue.h"

using namespace System::Runtime::InteropServices;

namespace SKManagedClient {


MAsyncRetrieval::~MAsyncRetrieval(void)
{
    this->!MAsyncRetrieval();
}

MAsyncRetrieval::!MAsyncRetrieval(void)
{
    if(pImpl) 
    {
        delete (SKAsyncRetrieval*)pImpl ; 
        pImpl = NULL;
    }
}

MAsyncRetrieval::MAsyncRetrieval(SKAsyncOperation_M ^ asyncRetrieval)
{
    pImpl = asyncRetrieval->pAsyncOperation;
}

SKAsyncOperation_M ^ MAsyncRetrieval::getPImpl()
{
    SKAsyncOperation_M ^ asyncRetrieval = gcnew SKAsyncOperation_M;
    asyncRetrieval->pAsyncOperation = pImpl;
    return asyncRetrieval;
}

MAsyncRetrieval::MAsyncRetrieval() 
{ 
    //
}

Dictionary<String ^ , MStoredValue ^ > ^  MAsyncRetrieval::retrieveStoredValues(bool latest)
{
    StrSVMap * vals =  NULL;

    if(latest) {
        vals = ((SKAsyncRetrieval*)pImpl)->getLatestStoredValues();
    }
    else
    {
        vals = ((SKAsyncRetrieval*)pImpl)->getStoredValues();
    }

    int nKeys = vals->size();
    Dictionary<String^, MStoredValue^>^ values = gcnew Dictionary<String^, MStoredValue^>(nKeys);

    StrSVMap::iterator it;
    for (it=vals->begin(); it!=vals->end(); vals++) {
        SKStoredValue * pval = it->second;
        String ^ skey = gcnew String( it->first.c_str());
        if( pval != NULL ){
            SKStoredValue_M ^ svm = gcnew SKStoredValue_M;
            svm->pStoredValue = (pval);
            MStoredValue ^ storedValue = gcnew MStoredValue(svm);
            values->Add(skey, storedValue);
        }else  {
            values->Add(skey, nullptr);
        }

    }

    delete vals ; vals = NULL ;
    return values;
}

//AsyncRetrieval
Dictionary<String ^ , MStoredValue ^ > ^  MAsyncRetrieval::getLatestStoredValues()
{
    return this->retrieveStoredValues(true);
}

Dictionary<String ^ , MStoredValue ^ > ^  MAsyncRetrieval::getStoredValues()
{
    return this->retrieveStoredValues(false);
}

MStoredValue ^  MAsyncRetrieval::getStoredValue(String ^ key)
{
    MStoredValue ^ storedValue = nullptr;
    char* key_c =  NULL;
    try { 
        key_c = (char*)(void*)Marshal::StringToHGlobalAnsi(key);
        std::string strKey = string(key_c);
        SKStoredValue * pval =  ((SKAsyncRetrieval*)pImpl)->getStoredValue(strKey);
        if( pval ){
            SKStoredValue_M ^ svm = gcnew SKStoredValue_M;
            svm->pStoredValue = pval;
            storedValue = gcnew MStoredValue(svm);
        }

    }
    finally {
        if (key_c) Marshal::FreeHGlobal(System::IntPtr(key_c));
    }

    return storedValue;
}


}

