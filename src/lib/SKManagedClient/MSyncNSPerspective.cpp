#include "StdAfx.h"
#include "MSyncNSPerspective.h"
#include "MNamespace.h"
#include "MNamespacePerspectiveOptions.h"
#include "MVersionConstraint.h"
#include "MVersionProvider.h"
#include "MGetOptions.h"
#include "MPutOptions.h"
#include "MWaitOptions.h"
#include "MRetrievalOptions.h"
#include "MVersionConstraint.h"
#include "MVersionProvider.h"
#include "MStoredValue.h"

#include <string>
using namespace std;
#include <stdlib.h>
#include "skconstants.h"
#include "skcontainers.h"
#include "SKNamespace.h"
#include "SKNamespacePerspectiveOptions.h"
#include "SKVersionConstraint.h"
#include "SKVersionProvider.h"
#include "SKSyncNSPerspective.h"
#include "SKGetOptions.h"
#include "SKPutOptions.h"
#include "SKWaitOptions.h"
#include "SKRetrievalOptions.h"
#include "SKVersionConstraint.h"
#include "SKVersionProvider.h"
#include "SKStoredValue.h"


using namespace System::Runtime::InteropServices;

namespace SKManagedClient {

MSyncNSPerspective::!MSyncNSPerspective()
{
    if(pImpl)
    {
        delete (SKSyncNSPerspective*)pImpl ;
        pImpl = NULL;
    }
}

MSyncNSPerspective::~MSyncNSPerspective()
{
    this->!MSyncNSPerspective();
}

MSyncNSPerspective::MSyncNSPerspective(SKSyncNSPerspective_M ^ snsp)
{
    pImpl = snsp->pSnsp;
}

SKSyncNSPerspective_M ^ MSyncNSPerspective::getPImpl()
{
    SKSyncNSPerspective_M ^ snsp = gcnew SKSyncNSPerspective_M;
    snsp->pSnsp = pImpl;
    return snsp;
}


//MBaseNSPerspective
String ^ MSyncNSPerspective::getName()
{
    std::string nsName = ((SKSyncNSPerspective*)pImpl)->getName();
    String ^ name = gcnew String(nsName.c_str());
    return name;
}

MNamespace ^ MSyncNSPerspective::getNamespace()
{
    SKNamespace * pNs = ((SKSyncNSPerspective*)pImpl)->getNamespace();
    SKNamespace_M ^ nsImpl = gcnew SKNamespace_M;
    nsImpl->pNamespace = pNs;
    MNamespace ^ ns = gcnew MNamespace(nsImpl);
    return ns;
}

MNamespacePerspectiveOptions ^ MSyncNSPerspective::getOptions()
{
    SKNamespacePerspectiveOptions * pNspo = ((SKSyncNSPerspective*)pImpl)->getOptions();
    SKNamespacePerspectiveOptions_M ^ nspoImpl = gcnew SKNamespacePerspectiveOptions_M;
    nspoImpl->pNspOptions = pNspo;
    MNamespacePerspectiveOptions ^ nspo = gcnew MNamespacePerspectiveOptions(nspoImpl);
    return nspo;
}

void MSyncNSPerspective::setOptions(MNamespacePerspectiveOptions ^ nspOptions)
{
    SKNamespacePerspectiveOptions * pNspOpt = (SKNamespacePerspectiveOptions*) (nspOptions->getPImpl()->pNspOptions);
    ((SKSyncNSPerspective*)pImpl)->setOptions(pNspOpt);
}

void MSyncNSPerspective::setDefaultRetrievalVersionConstraint(MVersionConstraint ^ vc)
{
    SKVersionConstraint * pVc = (SKVersionConstraint*) (vc->getPImpl()->pVersionConstraint);
    ((SKSyncNSPerspective*)pImpl)->setDefaultRetrievalVersionConstraint(pVc);
}

void MSyncNSPerspective::setDefaultVersionProvider(MVersionProvider ^ versionProvider)
{
    SKVersionProvider * pVp = (SKVersionProvider*) (versionProvider->getPImpl()->pVersionProvider);
    ((SKSyncNSPerspective*)pImpl)->setDefaultVersionProvider(pVp);
}

void MSyncNSPerspective::setDefaultVersion(Int64 version)
{
    ((SKSyncNSPerspective*)pImpl)->setDefaultVersion(version);
}

void MSyncNSPerspective::close()
{
    ((SKSyncNSPerspective*)pImpl)->close();
}


//protected
Dictionary<String ^ , String ^ > ^ MSyncNSPerspective::retrieve(HashSet<String ^> ^ keys, bool isWaitFor)
{
    Dictionary< String ^, String ^ > ^ values = gcnew Dictionary< String ^, String ^ >();
    SKVector<string> skKeys;
    System::Collections::Generic::IEnumerator<String^> ^ hse = keys->GetEnumerator();
    while(hse->MoveNext()){
        String^ val = hse->Current;
        char* val_n = (char*)(void*)Marshal::StringToHGlobalAnsi(val);
        skKeys.push_back( string(val_n) );
        Marshal::FreeHGlobal(System::IntPtr(val_n));
    }

    SKMap<string, SKVal*>  * vals = NULL;
    if(isWaitFor)
    {
        vals = ((SKSyncNSPerspective*)pImpl)->waitFor(&skKeys);
    }
    else
    {
        vals = ((SKSyncNSPerspective*)pImpl)->get(&skKeys);
    }

    if(vals) {
        for (StrValMap::iterator it=vals->begin(); it!=vals->end(); vals++) {
            SKVal * pval = it->second;
            String ^ key = gcnew String( it->first.c_str()) ;
            if( pval != NULL ){
                String ^ val = gcnew String ( (char*)(pval->m_pVal), 0, pval->m_len);
                values->Add(key, val);
                sk_destroy_val( &pval );
            }else  {
                values->Add(key, nullptr);
            }
        }
        delete vals;
    }
    return values;
}

//public
Dictionary<String ^ , String ^ > ^ MSyncNSPerspective::get(HashSet<String ^> ^ dhtKeys)
{
    return this->retrieve(dhtKeys, false);
}

Dictionary<String ^ , String ^ > ^ MSyncNSPerspective::waitFor(HashSet<String ^ > ^ dhtKeys)
{
    return this->retrieve(dhtKeys, true);
}

Dictionary<String ^ , MStoredValue ^ > ^ MSyncNSPerspective::retrieve(HashSet<String ^ > ^ dhtKeys, MRetrievalOptions ^ retrievalOptions, bool isWaitFor)
{
    Dictionary< String ^, MStoredValue ^ > ^ values = gcnew Dictionary< String ^, MStoredValue ^ >();
    SKVector<string> skKeys;
    System::Collections::Generic::IEnumerator<String^> ^ hse = dhtKeys->GetEnumerator();
    while(hse->MoveNext()){
        String^ val = hse->Current;
        char* val_n = (char*)(void*)Marshal::StringToHGlobalAnsi(val);
        skKeys.push_back( string(val_n) );
        Marshal::FreeHGlobal(System::IntPtr(val_n));
    }

    SKMap<string, SKStoredValue*> * vals = NULL;
    if(isWaitFor)
    {
        MWaitOptions ^ waitOptions = safe_cast<MWaitOptions^>(retrievalOptions);
        if(waitOptions)
        {
            SKWaitOptions * pWaitOptions = (SKWaitOptions *)(waitOptions->getPImpl()->pWaitOptions);
            vals = ((SKSyncNSPerspective*)pImpl)->waitFor(&skKeys, pWaitOptions);
        }
    }
    else
    {
        MGetOptions ^ getOptions = safe_cast<MGetOptions^>(retrievalOptions);
        if(getOptions)
        {
            SKGetOptions * pGetOpt = (SKGetOptions *)(getOptions->getPImpl()->pGetOptions);
            vals = ((SKSyncNSPerspective*)pImpl)->get(&skKeys, pGetOpt);
        }
    }

    if(vals) {
        for (StrSVMap::iterator it=vals->begin(); it!=vals->end(); it++) {
            SKStoredValue * pval = it->second;
            String ^ key = gcnew String( it->first.c_str()) ;
            if( pval != NULL ){
                SKStoredValue_M ^ storVal = gcnew SKStoredValue_M;
                storVal->pStoredValue = pval;
                MStoredValue ^ value = gcnew MStoredValue(storVal);
                values->Add(key, value);
            }else  {
                values->Add(key, nullptr);
            }
        }
        delete vals;
    }

    return values;
}

Dictionary<String ^ , MStoredValue ^ > ^ MSyncNSPerspective::get(HashSet<String ^> ^ dhtKeys, MGetOptions ^ getOptions)
{
    return this->retrieve(dhtKeys, safe_cast<MRetrievalOptions^>(getOptions), false);
}

Dictionary<String ^ , MStoredValue ^ > ^ MSyncNSPerspective::waitFor(HashSet<String ^ > ^ dhtKeys, MWaitOptions ^ waitOptions)
{
    return this->retrieve(dhtKeys, safe_cast<MRetrievalOptions^>(waitOptions), true);
}

String ^ MSyncNSPerspective::retrieve(String ^ key, bool isWaitFor)
{
    String ^ str = nullptr;
    char* key_n = (char*)(void*)Marshal::StringToHGlobalAnsi(key);
    SKVal * pVal = NULL;
    if(isWaitFor)
    {
        pVal = ((SKSyncNSPerspective*)pImpl)->waitFor(key_n);
    }
    else
    {
        pVal = ((SKSyncNSPerspective*)pImpl)->get(key_n);
    }
    Marshal::FreeHGlobal(System::IntPtr(key_n));
    if(pVal){
        str = gcnew String((char *)(pVal->m_pVal), 0, pVal->m_len);
        sk_destroy_val( &pVal );
    }
    return str;
}

String ^ MSyncNSPerspective::get(String ^ key)
{
    return this->retrieve(key, false);
}
String ^ MSyncNSPerspective::waitFor(String ^ key)
{
    return this->retrieve(key, true);
}

MStoredValue ^ MSyncNSPerspective::retrieve(String ^ key, MRetrievalOptions ^ retrievalOptions, bool isWaitFor)
{
    char* key_n = (char*)(void*)Marshal::StringToHGlobalAnsi(key);
    SKStoredValue * pVal = NULL;
    MStoredValue ^ result = nullptr;
    if(isWaitFor)
    {
        MWaitOptions ^ waitOptions = safe_cast<MWaitOptions^>(retrievalOptions);
        if(waitOptions)
        {
            SKWaitOptions * pWo = (SKWaitOptions *)(waitOptions->getPImpl()->pWaitOptions);
            pVal = ((SKSyncNSPerspective*)pImpl)->waitFor(key_n, pWo);
        }
    }
    else
    {
        MGetOptions ^ getOptions = safe_cast<MGetOptions^>(retrievalOptions);
        if(getOptions)
        {
            SKGetOptions * pGo = (SKGetOptions *)(getOptions->getPImpl()->pGetOptions);
            pVal = ((SKSyncNSPerspective*)pImpl)->get(key_n, pGo);
        }
    }

    if(pVal)
    {
        SKStoredValue_M ^ storedValImp = gcnew SKStoredValue_M;
        storedValImp->pStoredValue = pVal;
        result = gcnew MStoredValue(storedValImp);
    }

    return result;
}

MStoredValue ^ MSyncNSPerspective::get(String ^ key, MGetOptions ^ getOptions)
{
    return this->retrieve(key, safe_cast<MRetrievalOptions^>(getOptions), false);
}

MStoredValue ^ MSyncNSPerspective::waitFor(String ^  key, MWaitOptions ^ waitOptions)
{
    return this->retrieve(key, safe_cast<MRetrievalOptions^>(waitOptions), true);
}


void MSyncNSPerspective::put_(Dictionary<String ^ , String ^ > ^ dhtValues, MPutOptions ^ putOptions, bool hasOptions)
{
    StrValMap skmap;
    IDictionaryEnumerator ^ ide = dhtValues->GetEnumerator();
    while (ide->MoveNext())
    {
        String ^ val = ide->Value->ToString();
        char* key_n = (char*)(void*)Marshal::StringToHGlobalAnsi(ide->Key->ToString());
        char* val_n = (char*)(void*)Marshal::StringToHGlobalAnsi(val);
        SKVal * pval = sk_create_val();
        sk_set_val(pval, val->Length, (void*)val_n);
        skmap.insert(StrValMap::value_type(string(key_n), pval));
        Marshal::FreeHGlobal(System::IntPtr(key_n));
        Marshal::FreeHGlobal(System::IntPtr(val_n));
    }

    // main call, creates SKAsyncPut
    if(hasOptions)  //check if we have putOptions 
    {
        SKPutOptions * pPo = (SKPutOptions *)(putOptions->getPImpl()->pPutOptions);
        ((SKSyncNSPerspective*)pImpl)->put( &skmap, pPo );
    }
    else 
    {
        ((SKSyncNSPerspective*)pImpl)->put( &skmap );
    }

}


void MSyncNSPerspective::put(Dictionary<String ^ , String ^ > ^ dhtValues)
{
    return this->put_(dhtValues, nullptr, false);
}

void MSyncNSPerspective::put(Dictionary<String ^ , String ^ > ^ dhtValues, MPutOptions ^ putOptions)
{
    return this->put_(dhtValues, putOptions, true);
}

void MSyncNSPerspective::put_(String ^ key, String ^ value, MPutOptions ^ putOptions, bool hasOptions)
{
    char* key_n = (char*)(void*)Marshal::StringToHGlobalAnsi(key);
    char* val_n = (char*)(void*)Marshal::StringToHGlobalAnsi(value);
    SKVal * pval = sk_create_val();
    sk_set_val(pval, value->Length, (void*)val_n);


    if(hasOptions)
    {
        SKPutOptions * pPutOpt = (SKPutOptions *)(putOptions->getPImpl()->pPutOptions);
        // main call
        ((SKSyncNSPerspective*)pImpl)->put( key_n, pval, pPutOpt );
    }
    else
    {
        ((SKSyncNSPerspective*)pImpl)->put( key_n, pval );
    }

    //release c pointers
    Marshal::FreeHGlobal(System::IntPtr(key_n));
    Marshal::FreeHGlobal(System::IntPtr(val_n));
    // cleanup SKVal
    sk_destroy_val( &pval );
}

void MSyncNSPerspective::put(String ^ key, String ^ value, MPutOptions ^ putOptions)
{
    return this->put_(key, value, putOptions, true);
}

void MSyncNSPerspective::put(String ^ key, String ^ value)
{
    return this->put_(key, value, nullptr, false);
}

void MSyncNSPerspective::snapshot()
{
    return ((SKSyncNSPerspective*)pImpl)->snapshot();
}

void MSyncNSPerspective::snapshot(Int64 version)
{
    return ((SKSyncNSPerspective*)pImpl)->snapshot(version);
}

void MSyncNSPerspective::syncRequest()
{
    return ((SKSyncNSPerspective*)pImpl)->syncRequest();
}

void MSyncNSPerspective::syncRequest(Int64 version)
{
    return ((SKSyncNSPerspective*)pImpl)->syncRequest(version);
}


}

