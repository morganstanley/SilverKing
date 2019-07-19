#include "StdAfx.h"
#include "MNamespaceOptions.h"
#include "MPutOptions.h"
#include "MGetOptions.h"
#include "MWaitOptions.h"

#include <stdlib.h>
#include "skconstants.h"
#include "SKNamespaceOptions.h"
#include "SKPutOptions.h"
#include "SKGetOptions.h"
#include "SKWaitOptions.h"

namespace SKManagedClient 
{

MNamespaceOptions::~MNamespaceOptions() 
{
    this->!MNamespaceOptions();
}

MNamespaceOptions::!MNamespaceOptions() {
    if(pImpl) 
    {
        delete (SKNamespaceOptions*)pImpl;
        pImpl = NULL;
    }
}

//impl
MNamespaceOptions::MNamespaceOptions(SKNamespaceOptions_M ^ namespaceOptions){
    pImpl = namespaceOptions->pNsOptions;
}
SKNamespaceOptions_M ^ MNamespaceOptions::getPImpl() {
    SKNamespaceOptions_M ^ nsOpt = gcnew SKNamespaceOptions_M;
    nsOpt->pNsOptions = pImpl;
    return nsOpt;
}

MNamespaceOptions ^ MNamespaceOptions::parse(String ^ def) {
    SKNamespaceOptions * pNso = SKNamespaceOptions::parse((char *) System::Runtime::InteropServices::Marshal::StringToHGlobalAnsi(def).ToPointer());
    SKNamespaceOptions_M ^ nso = gcnew SKNamespaceOptions_M;
    nso->pNsOptions = pNso;
    return gcnew MNamespaceOptions(nso);
}

MNamespaceOptions::MNamespaceOptions(SKStorageType_M storageType, SKConsistency_M consistencyProtocol, 
                    SKVersionMode_M versionMode, SKRevisionMode_M revisionMode, MPutOptions ^ defaultPutOptions,
                            MGetOptions ^ defaultGetOptions, MWaitOptions ^ defaultWaitOptions,
                            int secondarySyncIntervalSeconds, int segmentSize, bool allowLinks ) 
{
    SKPutOptions * pPutOpt   = (SKPutOptions *)  (defaultPutOptions->getPImpl()->pPutOptions);
    SKGetOptions * pGetOpt   = (SKGetOptions *)  (defaultGetOptions->getPImpl()->pGetOptions);
    SKWaitOptions * pWaitOpt = (SKWaitOptions *) (defaultWaitOptions->getPImpl()->pWaitOptions);
    pImpl  = new SKNamespaceOptions (
            (SKStorageType::SKStorageType) storageType, (SKConsistency) consistencyProtocol, 
            (SKVersionMode) versionMode, (SKRevisionMode) revisionMode,
            pPutOpt, pGetOpt, pWaitOpt, secondarySyncIntervalSeconds, segmentSize, allowLinks
    );
}

MNamespaceOptions::MNamespaceOptions(SKStorageType_M storageType, SKConsistency_M consistencyProtocol, 
                    SKVersionMode_M versionMode, SKRevisionMode_M revisionMode, MPutOptions ^ defaultPutOptions,
                    MGetOptions ^ defaultGetOptions, MWaitOptions ^ defaultWaitOptions, 
                    int secondarySyncIntervalSeconds, int segmentSize )
{
    SKPutOptions * pPutOpt = (SKPutOptions *) (defaultPutOptions->getPImpl()->pPutOptions);
    SKGetOptions * pGetOpt   = (SKGetOptions *)  (defaultGetOptions->getPImpl()->pGetOptions);
    SKWaitOptions * pWaitOpt = (SKWaitOptions *) (defaultWaitOptions->getPImpl()->pWaitOptions);
    pImpl  = new SKNamespaceOptions (
            (SKStorageType::SKStorageType) storageType, (SKConsistency) consistencyProtocol, 
            (SKVersionMode) versionMode, (SKRevisionMode) revisionMode,
            pPutOpt, pGetOpt, pWaitOpt, secondarySyncIntervalSeconds, segmentSize );
}

MNamespaceOptions::MNamespaceOptions(SKStorageType_M storageType, SKConsistency_M consistencyProtocol, 
                            SKVersionMode_M versionMode, MPutOptions ^ defaultPutOptions,
                            MGetOptions ^ defaultGetOptions, MWaitOptions ^ defaultWaitOptions )
{
    SKPutOptions * pPutOpt = (SKPutOptions *) (defaultPutOptions->getPImpl()->pPutOptions);
    SKGetOptions * pGetOpt   = (SKGetOptions *)  (defaultGetOptions->getPImpl()->pGetOptions);
    SKWaitOptions * pWaitOpt = (SKWaitOptions *) (defaultWaitOptions->getPImpl()->pWaitOptions);
    pImpl  = new SKNamespaceOptions (
            (SKStorageType::SKStorageType) storageType, (SKConsistency) consistencyProtocol, 
            (SKVersionMode) versionMode, pPutOpt, pGetOpt, pWaitOpt);
}


SKConsistency_M MNamespaceOptions::getConsistencyProtocol() {
    return (SKConsistency_M) ((SKNamespaceOptions*)pImpl)->getConsistencyProtocol();
}

MPutOptions ^ MNamespaceOptions::getDefaultPutOptions() {
    SKPutOptions * pPut = ((SKNamespaceOptions*)pImpl)->getDefaultPutOptions();
    SKPutOptions_M ^ put = gcnew SKPutOptions_M;
    put->pPutOptions = pPut;
    return gcnew MPutOptions(put);
}

MGetOptions ^ MNamespaceOptions::getDefaultGetOptions() {
    SKGetOptions * pGet = ((SKNamespaceOptions*)pImpl)->getDefaultGetOptions();
    SKGetOptions_M ^ get = gcnew SKGetOptions_M;
    get->pGetOptions = pGet;
    return gcnew MGetOptions(get);
}

MWaitOptions ^ MNamespaceOptions::getDefaultWaitOptions() {
    SKWaitOptions * pWait = ((SKNamespaceOptions*)pImpl)->getDefaultWaitOptions();
    SKWaitOptions_M ^ wait = gcnew SKWaitOptions_M;
    wait->pWaitOptions = pWait;
    return gcnew MWaitOptions(wait);
}

SKRevisionMode_M MNamespaceOptions::getRevisionMode() {
    return (SKRevisionMode_M) ((SKNamespaceOptions*)pImpl)->getRevisionMode();
}

SKStorageType_M MNamespaceOptions::getStorageType() {
    return (SKStorageType_M) ((SKNamespaceOptions*)pImpl)->getStorageType();
}

SKVersionMode_M MNamespaceOptions::getVersionMode() {
    return (SKVersionMode_M) ((SKNamespaceOptions*)pImpl)->getVersionMode();
}

int MNamespaceOptions::getSegmentSize(){
    return (int) ((SKNamespaceOptions*)pImpl)->getSegmentSize();
}

bool MNamespaceOptions::getAllowLinks(){
    return ((SKNamespaceOptions*)pImpl)->getAllowLinks();
}

String ^ MNamespaceOptions::toString()  {
    char * pStr =  ((SKNamespaceOptions*)pImpl)->toString();
    System::String ^ str = gcnew System::String(pStr);
    free( pStr );
    return str;
}

bool MNamespaceOptions::equals(MNamespaceOptions ^ other)  {
    SKNamespaceOptions* pOther = (SKNamespaceOptions*)(other->getPImpl()->pNsOptions);
    return ((SKNamespaceOptions*)pImpl)->equals(pOther);
}

MNamespaceOptions ^ MNamespaceOptions::segmentSize(int segmentSize) {
    ((SKNamespaceOptions*)pImpl)->segmentSize(segmentSize);
    return this;
}

MNamespaceOptions ^ MNamespaceOptions::allowLinks(bool allowLinks){
    ((SKNamespaceOptions*)pImpl)->allowLinks(allowLinks);
    return this;
}

MNamespaceOptions ^ MNamespaceOptions::storageType(SKStorageType_M storageType) {
    ((SKNamespaceOptions*)pImpl)->storageType( (SKStorageType::SKStorageType) storageType);
    return this;
}

MNamespaceOptions ^ MNamespaceOptions::consistencyProtocol(SKConsistency_M consistencyProtocol) {
    ((SKNamespaceOptions*)pImpl)->consistencyProtocol( (SKConsistency) consistencyProtocol);
    return this;
}

MNamespaceOptions ^ MNamespaceOptions::versionMode(SKVersionMode_M versionMode) {
    ((SKNamespaceOptions*)pImpl)->versionMode( (SKVersionMode) versionMode);
    return this;
}

MNamespaceOptions ^ MNamespaceOptions::revisionMode(SKRevisionMode_M revisionMode) {
    ((SKNamespaceOptions*)pImpl)->revisionMode( (SKRevisionMode) revisionMode);
    return this;
}

MNamespaceOptions ^ MNamespaceOptions::defaultPutOptions(MPutOptions ^ defaultPutOptions) {
    SKPutOptions * pPutOpt = (SKPutOptions *)(defaultPutOptions->getPImpl()->pPutOptions);
    ((SKNamespaceOptions*)pImpl)->defaultPutOptions( pPutOpt );
    return this;
}

MNamespaceOptions ^ MNamespaceOptions::secondarySyncIntervalSeconds(int secondarySyncIntervalSeconds) {
    ((SKNamespaceOptions*)pImpl)->secondarySyncIntervalSeconds(secondarySyncIntervalSeconds);
    return this;
}

int MNamespaceOptions::getSecondarySyncIntervalSeconds(){
    return (int) ((SKNamespaceOptions*)pImpl)->getSecondarySyncIntervalSeconds();
}


}
