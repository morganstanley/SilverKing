#include "SKNamespaceOptions.h"
#include "SKPutOptions.h"
#include "SKInvalidationOptions.h"
#include "SKGetOptions.h"
#include "SKWaitOptions.h"
#include "jenumutil.h"
#include "skbasictypes.h"

#include <string.h>
#include <string>
using std::string;
#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using namespace jace;
#include "jace/proxy/java/lang/String.h"
using ::jace::proxy::java::lang::String;
#include "jace/proxy/java/lang/Object.h"
using ::jace::proxy::java::lang::Object;
#include "jace/proxy/com/ms/silverking/cloud/dht/NamespaceOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::NamespaceOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/StorageType.h"
using jace::proxy::com::ms::silverking::cloud::dht::StorageType;
#include "jace/proxy/com/ms/silverking/cloud/dht/RevisionMode.h"
using jace::proxy::com::ms::silverking::cloud::dht::RevisionMode;
#include "jace/proxy/com/ms/silverking/cloud/dht/ConsistencyProtocol.h"
using jace::proxy::com::ms::silverking::cloud::dht::ConsistencyProtocol;
#include "jace/proxy/com/ms/silverking/cloud/dht/NamespaceVersionMode.h"
using jace::proxy::com::ms::silverking::cloud::dht::NamespaceVersionMode;
#include "jace/proxy/com/ms/silverking/cloud/dht/PutOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::PutOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/InvalidationOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::InvalidationOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/GetOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::GetOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/WaitOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::WaitOptions;


RevisionMode* convertRevisionMode (SKRevisionMode revisionMode) {
	switch(revisionMode)
	{
		case NO_REVISIONS : 
			return new RevisionMode (RevisionMode::valueOf("NO_REVISIONS"));
		case UNRESTRICTED_REVISIONS: 
			return new RevisionMode (RevisionMode::valueOf("UNRESTRICTED_REVISIONS"));
		default: 
			throw std::exception(); //FIXME:
	}
}

/* ctors / dtors */
SKNamespaceOptions::SKNamespaceOptions(SKStorageType::SKStorageType storageType, 
						SKConsistency consistencyProtocol, 
						SKVersionMode versionMode, 
						SKRevisionMode revisionMode, 
						SKPutOptions * defaultPutOptions,
						SKInvalidationOptions * defaultInvalidationOptions,
						SKGetOptions * defaultGetOptions, 
						SKWaitOptions * defaultWaitOptions,
						int secondarySyncIntervalSeconds, 
						int segmentSize, 
                        int maxValueSize, 
						bool allowLinks )
{
	StorageType * pSt = ::getStorageType( storageType );
	RevisionMode * pRm = convertRevisionMode( revisionMode );
	ConsistencyProtocol * pCp = ::getConsistencyProtocol( consistencyProtocol );
	NamespaceVersionMode * pNvm = ::getVersionMode(versionMode);
	PutOptions * pPo = (PutOptions *) defaultPutOptions->getPImpl();  //FIXME: friend
	InvalidationOptions * pIo = (InvalidationOptions *) defaultInvalidationOptions->getPImpl();
	GetOptions * pGo = (GetOptions *) defaultGetOptions->getPImpl(); 
	WaitOptions * pWo = (WaitOptions *) defaultWaitOptions->getPImpl();
	//pImpl = new NamespaceOptions(java_new<NamespaceOptions>(*pSt, *pCp, *pNvm, *pRm, *pPo,     
    //    *pIo, *pGo, *pWo, secondarySyncIntervalSeconds, segmentSize, allowLinks)); 
    pImpl = NamespaceOptions::Factory::create(*pSt, *pCp, *pNvm, *pRm, *pPo,     
        *pIo, *pGo, *pWo, secondarySyncIntervalSeconds, segmentSize, maxValueSize, allowLinks);
      
	delete pSt;
	delete pCp;
	delete pRm;
	delete pNvm;
}

SKNamespaceOptions::SKNamespaceOptions(void * pNamespaceOptions)
	: pImpl(pNamespaceOptions) {}; 
	
SKNamespaceOptions::~SKNamespaceOptions()
{
	if(pImpl!=NULL) {
		NamespaceOptions * pNSOpts = (NamespaceOptions*)pImpl;
		delete pNSOpts; 
		pImpl = NULL;
	}
}

void * SKNamespaceOptions::getPImpl(){
	return pImpl;
}

///////

SKNamespaceOptions *  SKNamespaceOptions::storageType(SKStorageType::SKStorageType storageType) {
    StorageType * pSt = ::getStorageType(storageType);
	NamespaceOptions * pNsoImp = new NamespaceOptions(java_cast<NamespaceOptions>(
		((NamespaceOptions*)pImpl)->storageType(*pSt)
	)); 
    delete ((NamespaceOptions*)pImpl);
    pImpl = pNsoImp;
    delete pSt;
    return this;
}

SKNamespaceOptions *  SKNamespaceOptions::consistencyProtocol(SKConsistency consistencyProtocol) {
    ConsistencyProtocol * pCp = ::getConsistencyProtocol(consistencyProtocol);
	NamespaceOptions * pNsoImp = new NamespaceOptions(java_cast<NamespaceOptions>(
		((NamespaceOptions*)pImpl)->consistencyProtocol(*pCp)
	)); 
    delete ((NamespaceOptions*)pImpl);
    pImpl = pNsoImp;
    delete pCp;
    return this;
}

SKNamespaceOptions *  SKNamespaceOptions::versionMode(SKVersionMode versionMode) {
	NamespaceVersionMode * pNvm = ::getVersionMode(versionMode);
	NamespaceOptions * pNsoImp = new NamespaceOptions(java_cast<NamespaceOptions>(
		((NamespaceOptions*)pImpl)->versionMode(*pNvm)
	)); 
    delete ((NamespaceOptions*)pImpl);
    pImpl = pNsoImp;
    delete pNvm;
    return this;
}

SKNamespaceOptions * SKNamespaceOptions::revisionMode(SKRevisionMode revisionMode){
    RevisionMode * pRm = convertRevisionMode(revisionMode);
	NamespaceOptions * pNsoImp = new NamespaceOptions(java_cast<NamespaceOptions>(
		((NamespaceOptions*)pImpl)->revisionMode(*pRm)
	)); 
    delete ((NamespaceOptions*)pImpl);
    pImpl = pNsoImp;
    delete pRm;
    return this;
}

SKNamespaceOptions *  SKNamespaceOptions::defaultPutOptions(SKPutOptions * defaultPutOptions) {
	PutOptions * pPo = (PutOptions *) defaultPutOptions->getPImpl();  //FIXME: friend
	NamespaceOptions * pNsoImp = new NamespaceOptions(java_cast<NamespaceOptions>(
		((NamespaceOptions*)pImpl)->defaultPutOptions(*pPo)
	)); 
    delete ((NamespaceOptions*)pImpl);
    pImpl = pNsoImp;
    return this;
}

SKNamespaceOptions *  SKNamespaceOptions::defaultInvalidationOptions(SKInvalidationOptions * defaultInvalidationOptions) {
	InvalidationOptions * pIo = (InvalidationOptions *) defaultInvalidationOptions->getPImpl();  //FIXME: friend
	NamespaceOptions * pNsoImp = new NamespaceOptions(java_cast<NamespaceOptions>(
		((NamespaceOptions*)pImpl)->defaultInvalidationOptions(*pIo)
	)); 
    delete ((NamespaceOptions*)pImpl);
    pImpl = pNsoImp;
    return this;
}

SKNamespaceOptions *  SKNamespaceOptions::defaultGetOptions(SKGetOptions * defaultGetOptions) {
	GetOptions * pGo = (GetOptions *) defaultGetOptions->getPImpl();  //FIXME: friend
	NamespaceOptions * pNsoImp = new NamespaceOptions(java_cast<NamespaceOptions>(
		((NamespaceOptions*)pImpl)->defaultGetOptions(*pGo)
	)); 
    delete ((NamespaceOptions*)pImpl);
    pImpl = pNsoImp;
    return this;
}

SKNamespaceOptions *  SKNamespaceOptions::defaultWaitOptions(SKWaitOptions * defaultWaitOptions) {
	WaitOptions * pWo = (WaitOptions *) defaultWaitOptions->getPImpl();  //FIXME: friend
	NamespaceOptions * pNsoImp = new NamespaceOptions(java_cast<NamespaceOptions>(
		((NamespaceOptions*)pImpl)->defaultWaitOptions(*pWo)
	)); 
    delete ((NamespaceOptions*)pImpl);
    pImpl = pNsoImp;
    return this;
}

SKNamespaceOptions *  SKNamespaceOptions::secondarySyncIntervalSeconds(int secondarySyncIntervalSeconds) {
	NamespaceOptions * pNsoImp = new NamespaceOptions(java_cast<NamespaceOptions>(
		((NamespaceOptions*)pImpl)->secondarySyncIntervalSeconds(secondarySyncIntervalSeconds)
	)); 
    delete ((NamespaceOptions*)pImpl);
    pImpl = pNsoImp;
    return this;
}

SKNamespaceOptions *  SKNamespaceOptions::segmentSize(int segmentSize) {
	NamespaceOptions * pNsoImp = new NamespaceOptions(java_cast<NamespaceOptions>(
		((NamespaceOptions*)pImpl)->segmentSize(segmentSize)
	)); 
    delete ((NamespaceOptions*)pImpl);
    pImpl = pNsoImp;
    return this;
}

SKNamespaceOptions *  SKNamespaceOptions::allowLinks(bool allowLinks) {
	NamespaceOptions * pNsoImp = new NamespaceOptions(java_cast<NamespaceOptions>(
		((NamespaceOptions*)pImpl)->allowLinks(allowLinks)
	)); 
    delete ((NamespaceOptions*)pImpl);
    pImpl = pNsoImp;
    return this;
}

////////

SKStorageType::SKStorageType SKNamespaceOptions::getStorageType(){
	int  storageType = (int)((NamespaceOptions*)pImpl)->getStorageType().ordinal() ; 
	return static_cast<SKStorageType::SKStorageType> (storageType);
}

SKConsistency SKNamespaceOptions::getConsistencyProtocol(){
	int  consistency = (int)((NamespaceOptions*)pImpl)->getConsistencyProtocol().ordinal() ; 
	return static_cast<SKConsistency> (consistency);
}

SKVersionMode SKNamespaceOptions::getVersionMode(){
	int versionMode = (int)((NamespaceOptions*)pImpl)->getVersionMode().ordinal() ; 
	return static_cast<SKVersionMode> (versionMode);
}

SKRevisionMode SKNamespaceOptions::getRevisionMode(){
	int revisionMode = (int)((NamespaceOptions*)pImpl)->getRevisionMode().ordinal() ; 
	return static_cast<SKRevisionMode> (revisionMode);
}

SKPutOptions * SKNamespaceOptions::getDefaultPutOptions(){
	PutOptions * pPutOptions = new PutOptions( java_cast<PutOptions>(
					((NamespaceOptions*)pImpl)->getDefaultPutOptions())); 
	return new SKPutOptions(pPutOptions);
}

SKInvalidationOptions * SKNamespaceOptions::getDefaultInvalidationOptions(){
	InvalidationOptions * pInvalidationOptions = new InvalidationOptions( java_cast<InvalidationOptions>(
					((NamespaceOptions*)pImpl)->getDefaultInvalidationOptions())); 
	return new SKInvalidationOptions(pInvalidationOptions);
}

SKGetOptions * SKNamespaceOptions::getDefaultGetOptions() {
	GetOptions * pGetOptions  = new GetOptions ( java_cast<GetOptions >(
					((NamespaceOptions*)pImpl)->getDefaultGetOptions())); 
	return new SKGetOptions(pGetOptions);
}

SKWaitOptions * SKNamespaceOptions::getDefaultWaitOptions() {
	WaitOptions * pWaitOptions  = new WaitOptions ( java_cast<WaitOptions >(
					((NamespaceOptions*)pImpl)->getDefaultWaitOptions())); 
	return new SKWaitOptions(pWaitOptions);
}

int SKNamespaceOptions::getSecondarySyncIntervalSeconds()
{
    int secondarySyncIntervalSec = (int)((NamespaceOptions*)pImpl)->getSecondarySyncIntervalSeconds() ;
    return secondarySyncIntervalSec;
}

int SKNamespaceOptions::getSegmentSize() {
    int segmentSize = (int)((NamespaceOptions*)pImpl)->getSegmentSize() ;
    return segmentSize;
}

bool  SKNamespaceOptions::getAllowLinks() {
	bool allowLinks =  (bool)((NamespaceOptions*)pImpl)->getAllowLinks() ; 
    return allowLinks;
}

////////

char * SKNamespaceOptions::toString() const {
	string representation = (string)((NamespaceOptions*)pImpl)->toString(); 
	return skStrDup(representation.c_str(),__FILE__, __LINE__);
	
}

bool SKNamespaceOptions::equals(SKNamespaceOptions * pOther) const {
	NamespaceOptions* pNSO = (NamespaceOptions*)(pOther->pImpl);
	return (bool)((NamespaceOptions*)pImpl)->equals(*pNSO) ; 
}

/* static */
SKNamespaceOptions * SKNamespaceOptions::parse(const char * def){
	NamespaceOptions * pNSOpts = new NamespaceOptions(java_cast<NamespaceOptions>(
			NamespaceOptions::parse(java_new<String>((char *)def))));
	return new SKNamespaceOptions(pNSOpts);
}

////////

bool SKNamespaceOptions::isWriteOnce() const
{
	bool writeOnce =  (bool)((NamespaceOptions*)pImpl)->isWriteOnce() ; 
    return writeOnce;

}

SKNamespaceOptions * SKNamespaceOptions::asWriteOnce() const
{
	NamespaceOptions * pNsoImp = new NamespaceOptions(java_cast<NamespaceOptions>(
		((NamespaceOptions*)pImpl)->asWriteOnce()
	)); 
	return new SKNamespaceOptions(pNsoImp);
}

