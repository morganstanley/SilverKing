#include "SKSession.h"
#include "SKNamespace.h"
#include "SKSyncNSPerspective.h"
#include "SKAsyncNSPerspective.h"
#include "SKNamespaceOptions.h"
#include "SKNamespaceCreationOptions.h"
#include "SKNamespacePerspectiveOptions.h"
#include "SKNamespaceCreationException.h"
#include "SKClientException.h"
#include "SKNamespaceRecoverException.h"
#include "SKNamespaceDeletionException.h"
#include "SKPutOptions.h"
#include "SKGetOptions.h"
#include "SKWaitOptions.h"

#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using namespace jace;
#include "jace/JArray.h"
using jace::JArray;
#include "jace/proxy/types/JByte.h"
using jace::proxy::types::JByte;
#include "jace/proxy/java/lang/String.h"
using jace::proxy::java::lang::String;
#include "jace/proxy/java/lang/Class.h"
using jace::proxy::java::lang::Class;

typedef JArray< jace::proxy::types::JByte > ByteArray;

#include "jace/proxy/com/google/common/base/Throwables.h"
using jace::proxy::com::google::common::base::Throwables;
#include "jace/proxy/com/ms/silverking/cloud/dht/NamespaceOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::NamespaceOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/NamespaceCreationOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::NamespaceCreationOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/NamespacePerspectiveOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::NamespacePerspectiveOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/NamespacePerspectiveOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::NamespacePerspectiveOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/DHTSession.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::DHTSession;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/Namespace.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::Namespace;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/SynchronousNamespacePerspective.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::SynchronousNamespacePerspective;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/AsynchronousNamespacePerspective.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::AsynchronousNamespacePerspective;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/NamespaceCreationException.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::NamespaceCreationException;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/NamespaceDeletionException.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::NamespaceDeletionException;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/NamespaceRecoverException.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::NamespaceRecoverException;
#include "jace/proxy/com/ms/silverking/cloud/dht/PutOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::PutOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/GetOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::GetOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/WaitOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::WaitOptions;

#include "jace/proxy/java/lang/Throwable.h"
using jace::proxy::java::lang::Throwable;
#include <iostream>
using namespace std;

SKSession::SKSession(DHTSession * pDHTSession) { //FIXME
    pImpl = pDHTSession ;
}

SKSession::~SKSession() {
    if(pImpl) {
        DHTSession* pDHTSession = (DHTSession*)pImpl;
        delete pDHTSession;
        pImpl = NULL;
    }
};

DHTSession * SKSession::getPImpl(){
    return pImpl;
}

SKNamespaceCreationOptions * SKSession::getNamespaceCreationOptions() {
    try {
        NamespaceCreationOptions * pNsco = NULL;
        pNsco = new NamespaceCreationOptions(java_cast<NamespaceCreationOptions>(
            ((DHTSession*)pImpl)->getNamespaceCreationOptions()
        ));
        return new SKNamespaceCreationOptions(pNsco);
    } catch (Throwable &t){
        throw SKClientException( &t, __FILE__, __LINE__ );
    }
}

SKNamespaceOptions * SKSession::getDefaultNamespaceOptions() {
    try {
        NamespaceOptions * pNso = NULL;
        pNso = new NamespaceOptions(java_cast<NamespaceOptions>(
            ((DHTSession*)pImpl)->getDefaultNamespaceOptions()
        ));
        return new SKNamespaceOptions(pNso);
    } catch (Throwable &t){
        throw SKClientException( &t, __FILE__, __LINE__ );
    }
}

SKPutOptions * SKSession::getDefaultPutOptions()
{
    try {
        PutOptions * pPo = NULL;
        pPo = new PutOptions(java_cast<PutOptions>(
            ((DHTSession*)pImpl)->getDefaultPutOptions()
        ));
        return new SKPutOptions(pPo);
    } catch (Throwable &t){
        throw SKClientException( &t, __FILE__, __LINE__ );
    }
}

SKGetOptions * SKSession::getDefaultGetOptions()
{
    try {
        GetOptions * pGo = NULL;
        pGo = new GetOptions(java_cast<GetOptions>(
            ((DHTSession*)pImpl)->getDefaultGetOptions()
        ));
        return new SKGetOptions(pGo);
    } catch (Throwable &t){
        throw SKClientException( &t, __FILE__, __LINE__ );
    }
}

SKWaitOptions * SKSession::getDefaultWaitOptions()
{
    try {
        WaitOptions * pWo = NULL;
        pWo = new WaitOptions(java_cast<WaitOptions>(
            ((DHTSession*)pImpl)->getDefaultWaitOptions()
        ));
        return new SKWaitOptions(pWo);
    } catch (Throwable &t){
        throw SKClientException( &t, __FILE__, __LINE__ );
    }
}

SKNamespace * SKSession::createNamespace(const char * ns) {
    try {
        Namespace * pNs = NULL; 
        pNs = new Namespace(java_cast<Namespace>(
            ((DHTSession*)pImpl)->createNamespace(java_new<String>((char*)ns) )
        ));
        return new SKNamespace(pNs);
    } catch (NamespaceCreationException &e){
        throw SKNamespaceCreationException(&e , __FILE__, __LINE__ );
    } catch (Throwable &t){
        throw SKClientException( &t, __FILE__, __LINE__ );
    }
}

SKNamespace * SKSession::createNamespace(const char * ns, SKNamespaceOptions * nsOptions){
    try {
        NamespaceOptions * pNsOpt = (NamespaceOptions*) nsOptions->getPImpl();
        Namespace * pNs = NULL; 
        pNs = new Namespace(java_cast<Namespace>(
            ((DHTSession*)pImpl)->createNamespace(java_new<String>((char*)ns), *pNsOpt)
        ));
        return new SKNamespace(pNs);
    } catch (NamespaceCreationException &e){
        throw SKNamespaceCreationException(&e , __FILE__, __LINE__ );
    } catch (Throwable &t){
        throw SKClientException( &t, __FILE__, __LINE__ );
    }
}

SKNamespace * SKSession::getNamespace(const char * ns){
    try {
        Namespace * pNs = NULL;
        pNs = new Namespace(java_cast<Namespace>(
            ((DHTSession*)pImpl)->getNamespace(java_new<String>((char*)ns))
        ));
        return new SKNamespace(pNs);
    } catch (Throwable &t){
        throw SKClientException( &t, __FILE__, __LINE__ );
    }
}

SKAsyncNSPerspective * SKSession::openAsyncNamespacePerspective(const char * ns, 
                                SKNamespacePerspectiveOptions * pNspOptions) 
{
    try {
        NamespacePerspectiveOptions * pNSPOpt = (NamespacePerspectiveOptions*) pNspOptions->getPImpl();
        AsynchronousNamespacePerspective * pANsp = new AsynchronousNamespacePerspective(
            java_cast<AsynchronousNamespacePerspective>(
                ((DHTSession*)pImpl)->openAsyncNamespacePerspective(java_new<String>((char*)ns), *pNSPOpt)
        ));
        return new SKAsyncNSPerspective(pANsp);
    } catch (Throwable &t){
        throw SKClientException( &t, __FILE__, __LINE__ );
    }
    
}

SKAsyncNSPerspective * SKSession::openAsyncNamespacePerspective(const char * ns /*, KeyClass k, ValueClass v */)
{
    try {
        /* KeyClass k, ValueClass v */  // this part should be templetized either based on jace java wrapper classes (?) or on std c++ classes ()
        ByteArray byteArray(3);
        Class byteArryCls(byteArray.staticGetJavaJniClass().getClass());
        Class strCls(String("").staticGetJavaJniClass().getClass());

        AsynchronousNamespacePerspective * pANsp = new AsynchronousNamespacePerspective(
            java_cast<AsynchronousNamespacePerspective>(
                ((DHTSession*)pImpl)->openAsyncNamespacePerspective(java_new<String>(
                    (char*)ns), strCls, byteArryCls )
        ));
        return new SKAsyncNSPerspective(pANsp);
    } catch (Throwable &t){
        throw SKClientException( &t, __FILE__, __LINE__ );
    } catch (SKClientException &t){
        (void) t;  // get rid of VS2010 warning
        throw ;
    }
}

SKSyncNSPerspective * SKSession::openSyncNamespacePerspective(const char * ns,
                                SKNamespacePerspectiveOptions * pNspOptions)
{
    try {
        NamespacePerspectiveOptions * pNSPOpt = (NamespacePerspectiveOptions*) pNspOptions->getPImpl();
        SynchronousNamespacePerspective * pSNsp = new SynchronousNamespacePerspective(
            java_cast<SynchronousNamespacePerspective>(
                ((DHTSession*)pImpl)->openSyncNamespacePerspective(java_new<String>((char*)ns), *pNSPOpt)
        ));
        return new SKSyncNSPerspective(pSNsp);
    } catch (Throwable &t){
        throw SKClientException( &t, __FILE__, __LINE__ );
    }
    
}

SKSyncNSPerspective * SKSession::openSyncNamespacePerspective(const char * ns /*, KeyClass k, ValueClass v */)
{
    try {
        /* KeyClass k, ValueClass v */
        ByteArray byteArray(3);
        Class byteArryCls(byteArray.staticGetJavaJniClass().getClass());
        Class strCls(String("").staticGetJavaJniClass().getClass());

        SynchronousNamespacePerspective * pSNsp = new SynchronousNamespacePerspective(
            java_cast<SynchronousNamespacePerspective>(
                ((DHTSession*)pImpl)->openSyncNamespacePerspective(java_new<String>((char*)ns), strCls, byteArryCls )
        ));
        return new SKSyncNSPerspective(pSNsp);
    } catch (Throwable &t){
        throw SKClientException( &t, __FILE__, __LINE__ );
    } catch (SKClientException &t){
        (void) t;  // get rid of VS2010 warning
        throw ;
    }
}

void SKSession::close(){
    return ((DHTSession*)pImpl)->close();
}

void SKSession::deleteNamespace(const char * ns)
{
    try {
        ((DHTSession*)pImpl)->deleteNamespace(java_new<String>((char*)ns));
    } catch (NamespaceDeletionException &t){
        throw SKNamespaceDeletionException( &t, __FILE__, __LINE__ );
    } catch (Throwable &t){
        throw SKClientException( &t, __FILE__, __LINE__ );
    }
}

void SKSession::recoverNamespace(const char * ns)
{
    try {
        ((DHTSession*)pImpl)->recoverNamespace(java_new<String>((char*)ns));
    } catch (NamespaceRecoverException &t){
        throw SKNamespaceRecoverException( &t, __FILE__, __LINE__ );
    } catch (Throwable &t){
        throw SKClientException( &t, __FILE__, __LINE__ );
    }
}

