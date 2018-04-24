#include "AsyncPutAsyncGet.h"

#include "PutOpts.h"
#include "AsyncNSP.h"
#include "GetOpts.h"

AsyncPutAsyncGet::AsyncPutAsyncGet(const string& gc, const string& h, const string& n, const string& log, int verb, const string& nsOpts, const string& jvmOpts, SKCompression::SKCompression compress, long long int version, SKRetrievalType retrieval, const string& k, const string& v, int tmOut)
 : DhtAction(gc, h, n, log, verb, nsOpts, jvmOpts, compress, version, retrieval)
 , key(k)
 , value(v)
 , timeout(tmOut)
 , pPutOpt()
 , ansp()
 , pGetOpt()
 {
 }

AsyncPutAsyncGet::~AsyncPutAsyncGet() {
  delete pPutOpt;
  delete ansp;
  delete pGetOpt;
}

string
AsyncPutAsyncGet::getKey() {
  return key;
}

string
AsyncPutAsyncGet::getValue() {
  return value;
}

string
AsyncPutAsyncGet::getCompression() {
  return compress;
}

SKPutOptions*
AsyncPutAsyncGet::getPutOpt() {
  return pPutOpt;
}

SKAsyncNSPerspective*
AsyncPutAsyncGet::getAnsp() {
  return ansp;
}

SKGetOptions*
AsyncPutAsyncGet::getGetOpt() {
  return pGetOpt;
}

SKAsyncNSPerspective*
AsyncPutAsyncGet::getAsyncNSPerspective() {
  ansp = AsyncNSP::getAsyncNSPerspective(session, ns, pNspOptions);
  return ansp;
}

void
AsyncPutAsyncGet::put(string k, string v) {
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
    fprintf(stdout, "SKPutException in AsyncPutAsyncGet : %s" , pe.what() );
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
AsyncPutAsyncGet::put(string k, string v, SKPutOptions* pPutOpts) {
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

string
AsyncPutAsyncGet::get(string k) {
  SKVal* pval = NULL;
  string val;
  SKAsyncSingleValueRetrieval* pAsvr = NULL;
  try {
    pAsvr = ansp->get(&k);
    if(!pAsvr) {
      cout << "Failed to create SKAsyncSingleValueRetrieval with ansp->get(" << k << ")" << endl;
      exit (1);
    }
    pAsvr->waitForCompletion();
    if(pAsvr->getState() == SKOperationState::FAILED) {
      SKFailureCause::SKFailureCause reason = pAsvr->getFailureCause();
      cout << "SKAsyncSingleValueRetrieval: " << pAsvr << " failed, cause: " << reason << endl;
      exit(1);
    }
  } catch (SKClientException & ce ){
    exhandler( "caught in get", __FILE__, __LINE__, ns.c_str() );
  } catch (...){
    exhandler( "caught in get", __FILE__, __LINE__, ns.c_str() );
  }
  pval = pAsvr->getValue();
  if(pval){
    val = (char*) (pval->m_pVal);
    sk_destroy_val( &pval );
  } else {
    cout << "\"get\" namespace " << ns << ", key " << k << ", missing value" << endl;
  }
  return val;
}

SKGetOptions*
AsyncPutAsyncGet::getGetOptions() {
  pGetOpt = GetOpts::getGetOptions(ansp, retrievalType, valueVersion);
  applyGetOptsOnNSPOptions(pGetOpt);
  return pGetOpt;
}

SKNamespacePerspectiveOptions*
AsyncPutAsyncGet::applyGetOptsOnNSPOptions(SKGetOptions* gOpts) {
  pNspOptions = pNspOptions->defaultGetOptions(gOpts);
  return pNspOptions;
}

string
AsyncPutAsyncGet::get(string k, SKGetOptions* getOpts) {
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
    SKVal* pVal = pStoredVal->getValue();
    val = (char*)(pVal->m_pVal);
    sk_destroy_val(&pVal);
    delete pStoredVal;
  } else {
    cout << "\"get\" namespace " << ns << ", key " << k << ", missing value, version " << valueVersion << endl;
  }

  return val;
}

