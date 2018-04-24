#include "PutGetWait.h"

#include "PutOpts.h"
#include "SyncNSP.h"
#include "WaitOpts.h"

PutGetWait::PutGetWait(const string& gc, const string& h, const string& n, const string& log, int verb, const string& nsOpts, const string& jvmOpts, SKCompression::SKCompression compress, long long int version, SKRetrievalType retrieval, const string& k, const string& v, int tmOut, int thresh)
 : DhtAction(gc, h, n, log, verb, nsOpts, jvmOpts, compress, version, retrieval)
 , key(k)
 , value(v)
 , timeout(tmOut)
 , threshold(thresh)
 , pPutOpt()
 , snsp()
 {
 }

PutGetWait::~PutGetWait() {
  delete pPutOpt;
  delete snsp;
  delete waitOpt;
}

string
PutGetWait::getKey() {
  return key;
}

string
PutGetWait::getValue() {
  return value;
}

string
PutGetWait::getCompression() {
  return compress;
}

SKSyncNSPerspective*
PutGetWait::getSnsp() {
  return snsp;
}

SKWaitOptions*
PutGetWait::getWaitOpt() {
  return waitOpt;
}

SKSyncNSPerspective*
PutGetWait::getSyncNSPerspective() {
  snsp = SyncNSP::getSyncNSPerspective(session, ns, pNspOptions);
  return snsp;
}

void
PutGetWait::put(string k, string v) {
  SKVal* pval = sk_create_val();
  if(!v.empty()) {
    sk_set_val(pval, v.length(), (void *)v.data());
  }

  try {
    snsp->put(&k, pval);
  } catch (SKPutException & pe ){
    fprintf(stdout, "SKPutException in PutGetWait : %s" , pe.what() );
  } catch (SKClientException & ce ){
    exhandler( "caught in put", __FILE__, __LINE__, ns.c_str() );
  } catch (...){
    exhandler( "caught in put", __FILE__, __LINE__, ns.c_str() );
  }
  sk_destroy_val(&pval);
}

void
PutGetWait::put(string k, string v, SKPutOptions* pPutOpts) {
  SKVal* pval = sk_create_val();
  if(!v.empty()) {
    sk_set_val(pval, v.length(), (void *)v.data());
  }

  try {
    snsp->put(&k, pval, pPutOpts);
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
PutGetWait::getWaitOptions() {
  waitOpt = WaitOpts::getWaitOptions(snsp, retrievalType, valueVersion, timeout, threshold);
  return waitOpt;
}

string
PutGetWait::waitFor(string k, SKWaitOptions* opt) {
  SKStoredValue* pStoredVal = NULL;
  SKVal* pVal = NULL;

  string val;
  try {
    pStoredVal = snsp->waitFor(k.data(), opt);
    if(pStoredVal) {
      pVal = pStoredVal->getValue();
      if(pVal && pVal->m_len > 0) {
        val = string((char*)pVal->m_pVal, pVal->m_len);
        sk_destroy_val(&pVal);
      } else {
        cout << "NULL return for " << k << endl;
        exit(1);
      }
      delete pStoredVal;
    }
  } catch (SKClientException & ce ){
    exhandler( "caught in get", __FILE__, __LINE__, ns.c_str() );
  } catch (...){
    exhandler( "caught in get", __FILE__, __LINE__, ns.c_str() );
  }
  return val;
}

