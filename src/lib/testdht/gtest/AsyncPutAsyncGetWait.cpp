#include "AsyncPutAsyncGetWait.h"

#include "PutOpts.h"
#include "AsyncNSP.h"
#include "WaitOpts.h"

AsyncPutAsyncGetWait::AsyncPutAsyncGetWait(const string& gc, const string& h, const string& n, const string& log, int verb, const string& nsOpts, const string& jvmOpts, SKCompression::SKCompression compress, long long int version, SKRetrievalType retrieval, const string& k, const string& v, int tmOut, int thresh)
 : DhtAction(gc, h, n, log, verb, nsOpts, jvmOpts, compress, version, retrieval)
 , key(k)
 , value(v)
 , timeout(tmOut)
 , threshold(thresh)
 , pPutOpt()
 , ansp()
 , pWaitOpt()
 {
//   cout << "AsyncPutAsyncGetWait::AsyncPutAsyncGetWait(" << gc << ", " << h << ", " << n << ", " << log << ", " << verb  << ", " << nsOpts << ", " << jvmOpts << ", " << compress << ", " << version << ", " << retrieval << ", " << k << ", " << v << ")" << endl;
 }

AsyncPutAsyncGetWait::~AsyncPutAsyncGetWait() {
  delete pPutOpt;
  delete ansp;
  delete pWaitOpt;
}

string
AsyncPutAsyncGetWait::getKey() {
  return key;
}

string
AsyncPutAsyncGetWait::getValue() {
  return value;
}

string
AsyncPutAsyncGetWait::getCompression() {
  return compress;
}

SKAsyncNSPerspective*
AsyncPutAsyncGetWait::getAnsp() {
  return ansp;
}

SKWaitOptions*
AsyncPutAsyncGetWait::getWaitOpt() {
  return pWaitOpt;
}

int
AsyncPutAsyncGetWait::getTimeout() {
  return timeout;
}

int
AsyncPutAsyncGetWait::getThreshold() {
  return threshold;
}

SKAsyncNSPerspective*
AsyncPutAsyncGetWait::getAsyncNSPerspective() {
  ansp = AsyncNSP::getAsyncNSPerspective(session, ns, pNspOptions);
  return ansp;
}

void
AsyncPutAsyncGetWait::put(string k, string v) {
  SKVal* pval = sk_create_val();
  if(!v.empty()) {
    sk_set_val(pval, v.length(), (void *)v.data());
  }

  try {
    ansp->put(&k, pval);
  } catch (SKPutException & pe ){
    fprintf(stdout, "SKPutException in AsyncPutAsyncGetWait : %s" , pe.what() );
  } catch (SKClientException & ce ){
    exhandler( "caught in put", __FILE__, __LINE__, ns.c_str() );
  } catch (...){
    exhandler( "caught in put", __FILE__, __LINE__, ns.c_str() );
  }
  sk_destroy_val(&pval);
}

void
AsyncPutAsyncGetWait::put(string k, string v, SKPutOptions* pPutOpts) {
  SKVal* pval = sk_create_val();
  if(!v.empty()) {
    sk_set_val(pval, v.length(), (void *)v.data());
  }

  try {
    ansp->put(&k, pval, pPutOpts);
  } catch (SKPutException & pe ){
    fprintf(stdout, "SKPutException in put(key, value, pPutOpts) : %s" , pe.what() );
  } catch (SKClientException & ce ){
    exhandler("caught in put", __FILE__, __LINE__, ns.c_str() );
  } catch (...){
    exhandler("caught in put", __FILE__, __LINE__, ns.c_str() );
  }
  sk_destroy_val(&pval);
}

SKWaitOptions*
AsyncPutAsyncGetWait::getWaitOptions() {
  pWaitOpt = WaitOpts::getWaitOptions(ansp, retrievalType, valueVersion, timeout, threshold);
  return pWaitOpt;
}

string
AsyncPutAsyncGetWait::waitFor(string k) {
cout << "in APutAWaitHelloWorld::waitFor(" << k << ")" << endl;
  SKVal* pval = NULL;
  string val;
  SKAsyncSingleValueRetrieval* pAsvr = NULL;
  try {
    pAsvr = ansp->waitFor(&k);
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
    cout << "\"waitFor\" namespace " << ns << ", key " << k << ", missing value" << endl;
  }
  return val;
}

string
AsyncPutAsyncGetWait::waitFor(string k, SKWaitOptions* opt) {
  SKAsyncRetrieval* pAsyncRetrvlVal = NULL;
  SKStoredValue* pStoredVal = NULL;
  string val;
  try {
    pAsyncRetrvlVal = ansp->waitFor(&k, pWaitOpt);
    if(!pAsyncRetrvlVal){
        cout << "Failed to call ansp->get(" << key << ", pWaitOpt) to create pAsyncRetrvlVal" << endl;
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
    cout << "\"waitFor\" namespace " << ns << ", key " << k << ", missing value, version " << valueVersion << endl;
  }

  return val;
}

