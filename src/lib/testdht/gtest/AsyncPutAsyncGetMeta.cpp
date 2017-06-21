#include "AsyncPutAsyncGetMeta.h"

#include "PutOpts.h"
#include "AsyncNSP.h"
#include "GetOpts.h"

AsyncPutAsyncGetMeta::AsyncPutAsyncGetMeta(const string& gc, const string& h, const string& n, const string& log, int verb, const string& nsOpts, const string& jvmOpts, SKCompression::SKCompression compress, long long int version, SKRetrievalType retrieval, const string& k, const string& v, int tmOut)
 : DhtAction(gc, h, n, log, verb, nsOpts, jvmOpts, compress, version, retrieval)
 , key(k)
 , value(v)
 , timeout(tmOut)
 , pPutOpt()
 , ansp()
 , pGetOpt()
 {
 }

AsyncPutAsyncGetMeta::~AsyncPutAsyncGetMeta() {
  delete pPutOpt;
  delete ansp;
  delete pGetOpt;
}

string
AsyncPutAsyncGetMeta::getKey() {
  return key;
}

string
AsyncPutAsyncGetMeta::getValue() {
  return value;
}

string
AsyncPutAsyncGetMeta::getCompression() {
  return compress;
}

SKPutOptions*
AsyncPutAsyncGetMeta::getPutOpt() {
  return pPutOpt;
}

SKAsyncNSPerspective*
AsyncPutAsyncGetMeta::getAnsp() {
  return ansp;
}

SKGetOptions*
AsyncPutAsyncGetMeta::getMetaGetOpt() {
  return pGetOpt;
}

SKAsyncNSPerspective*
AsyncPutAsyncGetMeta::getAsyncNSPerspective() {
  ansp = AsyncNSP::getAsyncNSPerspective(session, ns, pNspOptions);
  return ansp;
}

void
AsyncPutAsyncGetMeta::put(string k, string v) {
  SKVal* pval = sk_create_val();
  if(!v.empty()) {
    sk_set_val(pval, v.length(), (void *)v.data());
  }

  SKAsyncPut* asyncPut = NULL;
  try {
    asyncPut = ansp->put(&k, pval);
    //ansp->waitForActiveOps();
    if(timeout == INT_MAX) {
      asyncPut->waitForCompletion();
    } else {
      bool done = asyncPut->waitForCompletion(timeout, SECONDS);
      if(!done) {
        if(asyncPut->getState()==SKOperationState::INCOMPLETE) {
          cout << "Async Put has not completed in " << timeout << endl;  
          asyncPut->waitForCompletion();
        }
      }
    }
  } catch (SKPutException & pe ){
    fprintf(stdout, "SKPutException in AsyncPutAsyncGetMeta : %s" , pe.what() );
  } catch (SKClientException & ce ){
    exhandler( "caught in put", __FILE__, __LINE__, ns.c_str() );
  } catch (...){
    exhandler( "caught in put", __FILE__, __LINE__, ns.c_str() );
  }
  asyncPut->close(); 
  delete asyncPut;
  sk_destroy_val(&pval);
}

void
AsyncPutAsyncGetMeta::put(string k, string v, SKPutOptions* pPutOpts) {
  SKVal* pval = sk_create_val();
  if(!v.empty()) {
    sk_set_val(pval, v.length(), (void *)v.data());
  }

  SKAsyncPut* asyncPut = NULL;
  try {
    asyncPut = ansp->put(&k, pval, pPutOpts);
    //ansp->waitForActiveOps();
    if(timeout == INT_MAX) {
      asyncPut->waitForCompletion();
    } else {
      bool done = asyncPut->waitForCompletion(timeout, SECONDS);
      if(!done) {
        if(asyncPut->getState()==SKOperationState::INCOMPLETE) {
          cout << "Async Put has not completed in " << timeout << endl;  
          asyncPut->waitForCompletion();
        }
      }
    }
  } catch (SKPutException & pe ){
    fprintf(stdout, "SKPutException in put(key, value, pPutOpts) : %s" , pe.what() );
  } catch (SKClientException & ce ){
    exhandler("caught in put", __FILE__, __LINE__, ns.c_str() );
  } catch (...){
    exhandler("caught in put", __FILE__, __LINE__, ns.c_str() );
  }
  asyncPut->close(); 
  delete asyncPut;
  sk_destroy_val(&pval);
}

SKGetOptions*
AsyncPutAsyncGetMeta::getMetaGetOptions() {
  pGetOpt = GetOpts::getMetaGetOptions(ansp, retrievalType, valueVersion);
  applyGetOptsOnNSPOptions(pGetOpt);
  return pGetOpt;
}

SKNamespacePerspectiveOptions*
AsyncPutAsyncGetMeta::applyGetOptsOnNSPOptions(SKGetOptions* gOpts) {
  pNspOptions = pNspOptions->defaultGetOptions(gOpts);
  return pNspOptions;
}

string
AsyncPutAsyncGetMeta::getMeta(string k, SKGetOptions* getOpts) {
  SKAsyncRetrieval* pAsyncRetrvlVal = NULL;
  SKStoredValue* pStoredVal = NULL;
  string val;
  try {
    pAsyncRetrvlVal = ansp->get(&k, getOpts);
    if(!pAsyncRetrvlVal){
        cout << "Failed to call ansp->get(" << key << ", getOpt) to create pAsyncRetrvlVal" << endl;
        exit(1);
    }
    pAsyncRetrvlVal->waitForCompletion();
    if(pAsyncRetrvlVal->getState()==SKOperationState::FAILED ) {
      SKFailureCause::SKFailureCause reason = pAsyncRetrvlVal->getFailureCause();
      fprintf( stderr, "AsyncValueRetrieval %p FailureCause : %d \n", pAsyncRetrvlVal, reason);
      exit(1);
    }
    pStoredVal = pAsyncRetrvlVal->getStoredValue(k);
  } catch (SKClientException & ce ){
    exhandler( "caught in get", __FILE__, __LINE__, ns.c_str() );
  } catch (...){
    exhandler( "caught in get", __FILE__, __LINE__, ns.c_str() );
  }

  if(pStoredVal){
    const char* metaData = pStoredVal->toString(true);
    if(metaData) {
      val = string(metaData);
      free((void*) metaData);
      metaData = NULL;
    }
    delete pStoredVal;
  } else {
    cout << "\"getMeta\" namespace " << ns << ", key " << k << ", missing value, version " << valueVersion << endl;
  }

  return val;
}

