#include "SKNamespace.h"
#include "SKSyncNSPerspective.h"
#include "SKAsyncNSPerspective.h"
#include "SKNamespaceOptions.h"
#include "SKNamespacePerspectiveOptions.h"
#include "SKNamespaceLinkException.h"
#include "SKNamespaceCreationException.h"
#include "skbasictypes.h"

#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using namespace jace;
#include "jace/proxy/types/JByte.h"
using jace::proxy::types::JByte;
#include "jace/proxy/types/JLong.h"
using jace::proxy::types::JLong;
#include "jace/JArray.h"
using jace::JArray;
typedef JArray< jace::proxy::types::JByte > ByteArray;

#include "jace/proxy/java/lang/Class.h"
using jace::proxy::java::lang::Class;
#include "jace/proxy/java/lang/Object.h"
using ::jace::proxy::java::lang::Object;
#include "jace/proxy/java/lang/String.h"
using jace::proxy::java::lang::String;
#include "jace/proxy/java/lang/Throwable.h"
using jace::proxy::java::lang::Throwable;
#include "jace/proxy/com/ms/silverking/cloud/dht/NamespaceOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::NamespaceOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/NamespacePerspectiveOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::NamespacePerspectiveOptions;

#include "jace/proxy/com/ms/silverking/cloud/dht/client/Namespace.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::Namespace;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/SynchronousNamespacePerspective.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::SynchronousNamespacePerspective;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/AsynchronousNamespacePerspective.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::AsynchronousNamespacePerspective;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/NamespaceCreationException.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::NamespaceCreationException;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/NamespaceLinkException.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::NamespaceLinkException;

SKNamespace::SKNamespace(Namespace * pOpt) : pImpl(pOpt) {};  //FIXME

SKNamespace::~SKNamespace()
{
    if(pImpl!=NULL) {
        delete pImpl; 
        pImpl = NULL;
    }
}

Namespace * SKNamespace::getPImpl(){
    return pImpl;
}

SKNamespacePerspectiveOptions * SKNamespace::getDefaultNSPOptions(){
    // <String, byte[]>
    ByteArray byteArray(3);
    Class byteArryCls(byteArray.staticGetJavaJniClass().getClass());
    Class strCls(String("").staticGetJavaJniClass().getClass());

    NamespacePerspectiveOptions * pSNPo = new NamespacePerspectiveOptions(
        java_cast<NamespacePerspectiveOptions>(((Namespace*)pImpl)->getDefaultNSPOptions(strCls, byteArryCls)) ); 
    return new SKNamespacePerspectiveOptions(pSNPo); 
}

SKNamespaceOptions * SKNamespace::getOptions(){
    NamespaceOptions * pNamespaceOptions = new NamespaceOptions(java_cast<NamespaceOptions>(
                                        ((Namespace*)pImpl)->getOptions())); 
    return new SKNamespaceOptions(pNamespaceOptions); 
}

SKAsyncNSPerspective * SKNamespace::openAsyncPerspective(SKNamespacePerspectiveOptions * nspOptions){
    try {
        NamespacePerspectiveOptions * pSNPo = (NamespacePerspectiveOptions*) nspOptions->getPImpl();
        AsynchronousNamespacePerspective * pANSp = new AsynchronousNamespacePerspective(
            java_cast<AsynchronousNamespacePerspective>(((Namespace*)pImpl)->openAsyncPerspective(*pSNPo)) ); 
        return new SKAsyncNSPerspective(pANSp); 
    } catch (Throwable &t){
        throw SKClientException( &t, __FILE__, __LINE__ );
    }
}

SKAsyncNSPerspective * SKNamespace::openAsyncPerspective(){
    try {
        /* KeyClass k, ValueClass v */  // this part should be templetized. Either based on jace java wrapper classes (?) or on std c++ classes ()
        ByteArray byteArray(3);
        Class byteArryCls(byteArray.staticGetJavaJniClass().getClass());
        Class strCls(String("").staticGetJavaJniClass().getClass());

        AsynchronousNamespacePerspective * pANSp = new AsynchronousNamespacePerspective(
            java_cast<AsynchronousNamespacePerspective>(((Namespace*)pImpl)->openAsyncPerspective(strCls, byteArryCls)) ); 
        return new SKAsyncNSPerspective(pANSp); 

    } catch (Throwable &t){
        throw SKClientException( &t, __FILE__, __LINE__ );
    } catch (SKClientException &t){
        (void) t;  // to get rid of VS2010 warning
        throw ;
    }
}

SKSyncNSPerspective * SKNamespace::openSyncPerspective(SKNamespacePerspectiveOptions * nspOptions){
    try {
        NamespacePerspectiveOptions * pSNPo = (NamespacePerspectiveOptions*) nspOptions->getPImpl();
        SynchronousNamespacePerspective * pSNSp = new SynchronousNamespacePerspective(
            java_cast<SynchronousNamespacePerspective>(((Namespace*)pImpl)->openSyncPerspective(*pSNPo)) ); 
        return new SKSyncNSPerspective(pSNSp); 
    } catch (NamespaceCreationException &e){
        throw SKNamespaceCreationException(&e , __FILE__, __LINE__ );
    } catch (Throwable &t){
        throw SKClientException( &t, __FILE__, __LINE__ );
    }
}

SKSyncNSPerspective * SKNamespace::openSyncPerspective(){
    try {
        /* KeyClass k, ValueClass v */  // this part should be templetized. Either based on jace java wrapper classes (?) or on std c++ classes ()
        ByteArray byteArray(3);
        Class byteArryCls(byteArray.staticGetJavaJniClass().getClass());
        Class strCls(String("").staticGetJavaJniClass().getClass());

        SynchronousNamespacePerspective * pSNSp = new SynchronousNamespacePerspective(
            java_cast<SynchronousNamespacePerspective>(((Namespace*)pImpl)->openSyncPerspective(strCls, byteArryCls)) ); 
        return new SKSyncNSPerspective(pSNSp); 
    } catch (Throwable &t){
        throw SKClientException( &t, __FILE__, __LINE__ );
    } catch (SKClientException &t){
        (void) t;  // to get rid of VS2010 warning
        throw ;
    }
}

char * SKNamespace::getName() {
      //string nsname = (string) ( ((Namespace*)pImpl)->getName() ); 
    string nsname = (string) ( java_cast<String>( ((Namespace*)pImpl)->getName() ) ); 
    return skStrDup(nsname.c_str(),__FILE__, __LINE__);
}

SKNamespace * SKNamespace::clone(const string &name) {
    try {
        Namespace * pChildNs = new Namespace( java_cast<Namespace>(
            ((Namespace*)pImpl)->clone(java_new<String>((char*)name.c_str()) ) 
        )); 
        return new SKNamespace(pChildNs); 
    } catch (NamespaceLinkException &e){
        throw SKNamespaceLinkException(&e , __FILE__, __LINE__ );
    } catch (Throwable &t){
        throw SKClientException( &t, __FILE__, __LINE__ );
    }
}

SKNamespace * SKNamespace::clone(const char * name) {
    try {
        Namespace * pChildNs = new Namespace( java_cast<Namespace>(
             ((Namespace*)pImpl)->clone(java_new<String>((char *)name) ) 
        )) ; 
        return new SKNamespace(pChildNs); 
    } catch (NamespaceLinkException &e){
        throw SKNamespaceLinkException(&e , __FILE__, __LINE__ );
    } catch (Throwable &t){
        throw SKClientException( &t, __FILE__, __LINE__ );
    }
}

SKNamespace * SKNamespace::clone(const string & name, int64_t version) {
    try {
        Namespace * pChildNs = new Namespace( java_cast<Namespace>(
            ((Namespace*)pImpl)->clone(java_new<String>((char*)name.c_str()), JLong(version) )
        )) ; 
        return new SKNamespace(pChildNs); 
    } catch (NamespaceLinkException &e){
        throw SKNamespaceLinkException(&e , __FILE__, __LINE__ );
    } catch (Throwable &t){
        throw SKClientException( &t, __FILE__, __LINE__ );
    }
}

SKNamespace * SKNamespace::clone(const char * name, int64_t version) {
    try {
        Namespace * pChildNs = new Namespace( java_cast<Namespace>(
            ((Namespace*)pImpl)->clone(java_new<String>((char *)name), JLong(version) )
        )) ; 
        return new SKNamespace(pChildNs); 
    } catch (NamespaceLinkException &e){
        throw SKNamespaceLinkException(&e , __FILE__, __LINE__ );
    } catch (Throwable &t){
        throw SKClientException( &t, __FILE__, __LINE__ );
    }
}

void SKNamespace::linkTo(const string & target) {
    try {
        ((Namespace*)pImpl)->linkTo(java_new<String>((char*)target.c_str())) ; 
    } catch (NamespaceLinkException &e){
        throw SKNamespaceLinkException(&e , __FILE__, __LINE__ );
    } catch (Throwable &t){
        throw SKClientException( &t, __FILE__, __LINE__ );
    }

};

void SKNamespace::linkTo(const char * target) {
    try {
        ((Namespace*)pImpl)->linkTo(java_new<String>((char *)target)) ; 
    } catch (NamespaceLinkException &e){
        throw SKNamespaceLinkException(&e , __FILE__, __LINE__ );
    } catch (Throwable &t){
        throw SKClientException( &t, __FILE__, __LINE__ );
    }
};


