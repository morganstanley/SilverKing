#include "StdAfx.h"
#include "MAsyncNSPerspective.h"
#include "MAsyncValueRetrieval.h"
#include "MAsyncRetrieval.h"
#include "MAsyncSingleValueRetrieval.h"
#include "MAsyncValueRetrieval.h"
#include "MAsyncPut.h"
#include "MAsyncSnapshot.h"
#include "MAsyncSyncRequest.h"
#include "MNamespace.h"
#include "MNamespacePerspectiveOptions.h"
#include "MGetOptions.h"
#include "MWaitOptions.h"
#include "MPutOptions.h"
#include "MRetrievalOptions.h"
#include "MVersionConstraint.h"
#include "MVersionProvider.h"

#include <stdlib.h>
#include <string>
using namespace std;
#include "skconstants.h"
#include "skcontainers.h"
#include "SKAsyncNSPerspective.h"
#include "SKAsyncValueRetrieval.h"
#include "SKAsyncRetrieval.h"
#include "SKAsyncSingleValueRetrieval.h"
#include "SKAsyncValueRetrieval.h"
#include "SKAsyncPut.h"
#include "SKAsyncSnapshot.h"
#include "SKAsyncSyncRequest.h"
#include "SKNamespace.h"
#include "SKNamespacePerspectiveOptions.h"
#include "SKVersionConstraint.h"
#include "SKVersionProvider.h"
#include "SKGetOptions.h"
#include "SKWaitOptions.h"
#include "SKPutOptions.h"
#include "SKRetrievalOptions.h"

using namespace System::Runtime::InteropServices;

namespace SKManagedClient {

MAsyncNSPerspective::!MAsyncNSPerspective()
{
    if(pImpl)
    {
        delete (SKAsyncNSPerspective*)pImpl ;
        pImpl = NULL;
    }
}

MAsyncNSPerspective::~MAsyncNSPerspective()
{
    this->!MAsyncNSPerspective();
}

MAsyncNSPerspective::MAsyncNSPerspective(SKAsyncNSPerspective_M ^ ansp)
{
    pImpl = ansp->pAnsp;
}

SKAsyncNSPerspective_M ^ MAsyncNSPerspective::getPImpl()
{
    SKAsyncNSPerspective_M ^ ansp = gcnew SKAsyncNSPerspective_M;
    ansp->pAnsp = pImpl;
    return ansp;
}


//MBaseNSPerspective
String ^ MAsyncNSPerspective::getName()
{
    std::string nsName = ((SKAsyncNSPerspective*)pImpl)->getName();
    String ^ name = gcnew String(nsName.c_str());
    return name;
}

MNamespace ^ MAsyncNSPerspective::getNamespace()
{
    SKNamespace * pNs = ((SKAsyncNSPerspective*)pImpl)->getNamespace();
    SKNamespace_M ^ nsImpl = gcnew SKNamespace_M;
    nsImpl->pNamespace = pNs;
    MNamespace ^ ns = gcnew MNamespace(nsImpl);
    return ns;
}

MNamespacePerspectiveOptions ^ MAsyncNSPerspective::getOptions()
{
    SKNamespacePerspectiveOptions * pNspo = ((SKAsyncNSPerspective*)pImpl)->getOptions();
    SKNamespacePerspectiveOptions_M ^ nspoImpl = gcnew SKNamespacePerspectiveOptions_M;
    nspoImpl->pNspOptions = pNspo;
    MNamespacePerspectiveOptions ^ nspo = gcnew MNamespacePerspectiveOptions(nspoImpl);
    return nspo;
}

void MAsyncNSPerspective::setOptions(MNamespacePerspectiveOptions ^ nspOptions)
{
    SKNamespacePerspectiveOptions * pNspOpt = (SKNamespacePerspectiveOptions*) (nspOptions->getPImpl()->pNspOptions);
    ((SKAsyncNSPerspective*)pImpl)->setOptions(pNspOpt);
}

void MAsyncNSPerspective::setDefaultRetrievalVersionConstraint(MVersionConstraint ^ vc)
{
    SKVersionConstraint * pVc = (SKVersionConstraint*) (vc->getPImpl()->pVersionConstraint);
    ((SKAsyncNSPerspective*)pImpl)->setDefaultRetrievalVersionConstraint(pVc);
}

void MAsyncNSPerspective::setDefaultVersionProvider(MVersionProvider ^ versionProvider)
{
    SKVersionProvider * pVp = (SKVersionProvider*) (versionProvider->getPImpl()->pVersionProvider);
    ((SKAsyncNSPerspective*)pImpl)->setDefaultVersionProvider(pVp);
}

void MAsyncNSPerspective::setDefaultVersion(Int64 version)
{
    ((SKAsyncNSPerspective*)pImpl)->setDefaultVersion(version);
}

void MAsyncNSPerspective::close()
{
    ((SKAsyncNSPerspective*)pImpl)->close();
}


//
MAsyncValueRetrieval ^ MAsyncNSPerspective::retrieve(HashSet<String ^> ^ dhtKeys, bool isWaitFor)
{
    SKVector<string> skKeys;
    System::Collections::Generic::IEnumerator<String^> ^ hse = dhtKeys->GetEnumerator();
    while(hse->MoveNext()){
        String^ val = hse->Current;
        char* val_n = (char*)(void*)Marshal::StringToHGlobalAnsi(val);
        skKeys.push_back( string(val_n) );
        Marshal::FreeHGlobal(System::IntPtr(val_n));
    }

    SKAsyncValueRetrieval * pRetrieval = NULL;
    MAsyncValueRetrieval ^ retr = nullptr;
    if(isWaitFor)
    {
        pRetrieval = ((SKAsyncNSPerspective*)pImpl)->waitFor(&skKeys);
    }
    else
    {
        pRetrieval = ((SKAsyncNSPerspective*)pImpl)->get(&skKeys);
    }

    if(pRetrieval)
    {
        SKAsyncOperation_M ^ retrImp = gcnew SKAsyncOperation_M;
        retrImp->pAsyncOperation = pRetrieval;
        retr = gcnew MAsyncValueRetrieval(retrImp);
    }
    return retr;
}

MAsyncValueRetrieval ^ MAsyncNSPerspective::get(HashSet<String ^> ^ dhtKeys)
{
    return this->retrieve(dhtKeys, false);
}

MAsyncValueRetrieval ^ MAsyncNSPerspective::waitFor(HashSet<String ^ > ^ dhtKeys)
{
    return this->retrieve(dhtKeys, true);
}

MAsyncRetrieval ^ MAsyncNSPerspective::retrieve(HashSet<String ^ > ^ dhtKeys, MRetrievalOptions ^ retrievalOptions, bool isWaitFor)
{
    SKVector<string> skKeys;
    System::Collections::Generic::IEnumerator<String^> ^ hse = dhtKeys->GetEnumerator();
    while(hse->MoveNext()){
        String^ val = hse->Current;
        char* val_n = (char*)(void*)Marshal::StringToHGlobalAnsi(val);
        skKeys.push_back( string(val_n) );
        Marshal::FreeHGlobal(System::IntPtr(val_n));
    }
    SKAsyncRetrieval * pRetrieval = NULL;
    MAsyncRetrieval ^ retr = nullptr;

    if(isWaitFor)
    {
        MWaitOptions ^ waitOptions = safe_cast<MWaitOptions^>(retrievalOptions);
        if(waitOptions)
        {
            SKWaitOptions * pWo = (SKWaitOptions *)(waitOptions->getPImpl()->pWaitOptions);
            pRetrieval = ((SKAsyncNSPerspective*)pImpl)->waitFor(&skKeys, pWo);
        }
    }
    else
    {
        MGetOptions ^ getOptions = safe_cast<MGetOptions^>(retrievalOptions);
        if(getOptions)
        {
            SKGetOptions * pGo = (SKGetOptions *)(getOptions->getPImpl()->pGetOptions);
            pRetrieval = ((SKAsyncNSPerspective*)pImpl)->get(&skKeys, pGo);
        }
    }

    if(pRetrieval)
    {
        SKAsyncOperation_M ^ retrImp = gcnew SKAsyncOperation_M;
        retrImp->pAsyncOperation = pRetrieval;
        retr = gcnew MAsyncRetrieval(retrImp);
    }
    return retr;
}

MAsyncRetrieval ^ MAsyncNSPerspective::waitFor(HashSet<String ^ > ^ dhtKeys, MWaitOptions ^ waitOptions)
{
    return this->retrieve(dhtKeys, safe_cast<MRetrievalOptions^>(waitOptions), true);
}

MAsyncRetrieval ^ MAsyncNSPerspective::get(HashSet<String ^> ^ dhtKeys, MGetOptions ^ getOptions)
{
    return this->retrieve(dhtKeys, safe_cast<MRetrievalOptions^>(getOptions), false);
}


MAsyncRetrieval ^ MAsyncNSPerspective::retrieve(String ^ key, MRetrievalOptions ^ retrievalOptions, bool isWaitFor)
{
    char* key_n = (char*)(void*)Marshal::StringToHGlobalAnsi(key);

    SKAsyncRetrieval * pRetrieval = NULL;
    MAsyncRetrieval ^ retr = nullptr;

    if(isWaitFor)
    {
        MWaitOptions ^ waitOptions = safe_cast<MWaitOptions^>(retrievalOptions);
        if(waitOptions)
        {
            SKWaitOptions * pWo = (SKWaitOptions *)(waitOptions->getPImpl()->pWaitOptions);
            pRetrieval = ((SKAsyncNSPerspective*)pImpl)->waitFor(key_n, pWo);
        }
    }
    else
    {
        MGetOptions ^ getOptions = safe_cast<MGetOptions^>(retrievalOptions);
        if(getOptions)
        {
            SKGetOptions * pGo = (SKGetOptions *)(getOptions->getPImpl()->pGetOptions);
            pRetrieval = ((SKAsyncNSPerspective*)pImpl)->get(key_n, pGo);
        }
    }

    Marshal::FreeHGlobal(System::IntPtr(key_n));
    if(pRetrieval)
    {
        SKAsyncOperation_M ^ retrImp = gcnew SKAsyncOperation_M;
        retrImp->pAsyncOperation = pRetrieval;
        retr = gcnew MAsyncRetrieval(retrImp);
    }
    return retr;
}

MAsyncRetrieval ^ MAsyncNSPerspective::waitFor(String ^  key, MWaitOptions ^ waitOptions)
{
    return this->retrieve(key, safe_cast<MRetrievalOptions^>(waitOptions), true);
}

MAsyncRetrieval ^ MAsyncNSPerspective::get(String ^ key, MGetOptions ^ getOptions)
{
    return this->retrieve(key, safe_cast<MRetrievalOptions^>(getOptions), false);
}

MAsyncSingleValueRetrieval ^ MAsyncNSPerspective::retrieve(String ^ key, bool isWaitFor)
{
    char* key_n = (char*)(void*)Marshal::StringToHGlobalAnsi(key);
    SKAsyncSingleValueRetrieval * pRetrieval = NULL;
    MAsyncSingleValueRetrieval ^ retr = nullptr;
    if(isWaitFor)
    {
        pRetrieval = ((SKAsyncNSPerspective*)pImpl)->waitFor(key_n);
    }
    else
    {
        pRetrieval = ((SKAsyncNSPerspective*)pImpl)->get(key_n);
    }
    
    Marshal::FreeHGlobal(System::IntPtr(key_n));

    if(pRetrieval)
    {
        SKAsyncOperation_M ^ retrImp = gcnew SKAsyncOperation_M;
        retrImp->pAsyncOperation = pRetrieval;
        retr = gcnew MAsyncSingleValueRetrieval(retrImp);
    }
    return retr;
}

MAsyncSingleValueRetrieval ^ MAsyncNSPerspective::get(String ^ key)
{
    return this->retrieve(key, false);
}

MAsyncSingleValueRetrieval ^ MAsyncNSPerspective::waitFor(String ^ key)
{
    return this->retrieve(key, true);
}

MAsyncPut ^ MAsyncNSPerspective::put_(Dictionary<String ^ , String ^ > ^ dhtValues, MPutOptions ^ putOptions, bool hasOptions)
{
    SKMap<string, SKVal*> skmap;
    IDictionaryEnumerator ^ ide = dhtValues->GetEnumerator();
    while (ide->MoveNext())
    {
        char* key_n = (char*)(void*)Marshal::StringToHGlobalAnsi(ide->Key->ToString());
        char* val_n = (char*)(void*)Marshal::StringToHGlobalAnsi(ide->Value->ToString());
        SKVal * pval = sk_create_val();
        sk_set_val(pval, ide->Value->ToString()->Length, (void*)val_n);
        skmap.insert(StrValMap::value_type(string(key_n), pval));
        Marshal::FreeHGlobal(System::IntPtr(key_n));
        Marshal::FreeHGlobal(System::IntPtr(val_n));
    }

    SKAsyncPut * pPut = NULL;
    MAsyncPut ^ aput = nullptr;

    if(hasOptions)
    {
        SKPutOptions * pPo = (SKPutOptions *)(putOptions->getPImpl()->pPutOptions);
        // main call, creates SKAsyncPut
        pPut = ((SKAsyncNSPerspective*)pImpl)->put( &skmap, pPo );
    }
    else
    {
        pPut = ((SKAsyncNSPerspective*)pImpl)->put( &skmap );
    }

    if(pPut)
    {
        // create CLR's MAsyncPut object
        SKAsyncOperation_M ^ putImp = gcnew SKAsyncOperation_M;
        putImp->pAsyncOperation = pPut;
        aput = gcnew MAsyncPut(putImp);
    }

    // cleanup SKVal's in the map
    StrValMap::iterator it;
    for (it=skmap.begin(); it!=skmap.end(); it++) {
        if(  it->second != NULL ){
            sk_destroy_val( &(it->second) );
        }
    }

    return aput;
}

MAsyncPut ^ MAsyncNSPerspective::put(Dictionary<String ^ , String ^ > ^ dhtValues, MPutOptions ^ putOptions)
{
    return this->put_(dhtValues, putOptions, true);
}

MAsyncPut ^ MAsyncNSPerspective::put(Dictionary<String ^ , String ^ > ^ dhtValues)
{
    return this->put_(dhtValues, nullptr, false);
}

MAsyncPut ^ MAsyncNSPerspective::put_(String ^ key, String ^ value, MPutOptions ^ putOptions, bool hasOptions)
{
    char* key_n = (char*)(void*)Marshal::StringToHGlobalAnsi(key);
    char* val_n = (char*)(void*)Marshal::StringToHGlobalAnsi(value);
    SKVal * pval = sk_create_val();
    sk_set_val(pval, value->Length, (void*)val_n);

    SKAsyncPut * pPut = NULL;
    MAsyncPut ^ aput = nullptr;
    if(hasOptions)
    {
        SKPutOptions * pPutOpt = (SKPutOptions *)(putOptions->getPImpl()->pPutOptions);
        // main call, creates SKAsyncPut
        pPut = ((SKAsyncNSPerspective*)pImpl)->put( key_n, pval, pPutOpt );
    }
    else
    {
        pPut = ((SKAsyncNSPerspective*)pImpl)->put( key_n, pval );
    }

    //release c pointers
    Marshal::FreeHGlobal(System::IntPtr(key_n));
    Marshal::FreeHGlobal(System::IntPtr(val_n));
    // cleanup SKVal
    sk_destroy_val( &pval );

    if(pPut)
    {
        // create CLR's MAsyncPut object
        SKAsyncOperation_M ^ putImp = gcnew SKAsyncOperation_M;
        putImp->pAsyncOperation = pPut;
        aput = gcnew MAsyncPut(putImp);
    }

    return aput;
}

MAsyncPut ^ MAsyncNSPerspective::put(String ^ key, String ^ value, MPutOptions ^ putOptions)
{
    return this->put_(key, value, putOptions, true);
}

MAsyncPut ^ MAsyncNSPerspective::put(String ^ key, String ^ value)
{
    return this->put_(key, value, nullptr, false);
}

MAsyncSnapshot ^ MAsyncNSPerspective::snapshot()
{
    SKAsyncSnapshot * pAsyncSnapshot = ((SKAsyncNSPerspective*)pImpl)->snapshot();
    // create CLR's MAsyncSnapshot object
    SKAsyncOperation_M ^ snapshotImp = gcnew SKAsyncOperation_M;
    snapshotImp->pAsyncOperation = pAsyncSnapshot;
    MAsyncSnapshot ^ asnapshot = gcnew MAsyncSnapshot(snapshotImp);

    return asnapshot;
}

MAsyncSnapshot ^ MAsyncNSPerspective::snapshot(Int64 version)
{
    SKAsyncSnapshot * pAsyncSnapshot = ((SKAsyncNSPerspective*)pImpl)->snapshot(version);
    // create CLR's MAsyncSnapshot object
    SKAsyncOperation_M ^ snapshotImp = gcnew SKAsyncOperation_M;
    snapshotImp->pAsyncOperation = pAsyncSnapshot;
    MAsyncSnapshot ^ asnapshot = gcnew MAsyncSnapshot(snapshotImp);

    return asnapshot;
}

MAsyncSyncRequest ^ MAsyncNSPerspective::syncRequest()
{
    SKAsyncSyncRequest * pSyncRequest = ((SKAsyncNSPerspective*)pImpl)->syncRequest();
    // create CLR's MAsyncSyncRequest object
    SKAsyncOperation_M ^ syncRequestImp = gcnew SKAsyncOperation_M;
    syncRequestImp->pAsyncOperation = pSyncRequest;
    MAsyncSyncRequest ^ syncRequest = gcnew MAsyncSyncRequest(syncRequestImp);

    return syncRequest;
}

MAsyncSyncRequest ^ MAsyncNSPerspective::syncRequest(Int64 version)
{
    SKAsyncSyncRequest * pSyncRequest = ((SKAsyncNSPerspective*)pImpl)->syncRequest(version);
    // create CLR's MAsyncSyncRequest object
    SKAsyncOperation_M ^ syncRequestImp = gcnew SKAsyncOperation_M;
    syncRequestImp->pAsyncOperation = pSyncRequest;
    MAsyncSyncRequest ^ syncRequest = gcnew MAsyncSyncRequest(syncRequestImp);

    return syncRequest;
}

}

