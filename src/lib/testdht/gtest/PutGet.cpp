#include "PutGet.h"

#include "PutOpts.h"
#include "SyncNSP.h"
#include "GetOpts.h"

PutGet::PutGet(const string& gc, const string& h, const string& n, const string& log, int verb, const string& nsOpts, const string& jvmOpts, SKCompression::SKCompression compress, long long int version, SKRetrievalType retrieval, const string& k, const string& v)
 : DhtAction(gc, h, n, log, verb, nsOpts, jvmOpts, compress, version, retrieval)
 , key(k)
 , value(v)
 , pPutOpt()
 , snsp()
 , pGetOpt()
 {
 }

PutGet::~PutGet() {
  delete pPutOpt;
  delete snsp;
  delete pGetOpt;
}

string
PutGet::getKey() {
  return key;
}

string
PutGet::getValue() {
  return value;
}

string
PutGet::getCompression() {
  return compress;
}

/*
SKPutOptions*
PutGet::getPutOpt() {
  return pPutOpt;
}
*/

SKSyncNSPerspective*
PutGet::getSnsp() {
  return snsp;
}

SKGetOptions*
PutGet::getGetOpt() {
  return pGetOpt;
}

/*
SKPutOptions*
PutGet::getPutOptions() {
  pPutOpt = PutOpts::getPutOptions(pNspOptions, ns, compressType, valueVersion, userData);
  return pPutOpt;
}
*/

SKSyncNSPerspective*
PutGet::getSyncNSPerspective() {
  snsp = SyncNSP::getSyncNSPerspective(session, ns, pNspOptions);
  return snsp;
}

void
PutGet::put(string k, string v) {
  SKVal* pval = sk_create_val();
  if(!v.empty()) {
    sk_set_val(pval, v.length(), (void *)v.data());
  }

  try {
    Util::HighResolutionClock log("PutGet-put-sync");
    snsp->put(&k, pval);
  } catch (SKPutException & pe ){
    fprintf(stdout, "SKPutException in PutGet : %s" , pe.what() );
  } catch (SKClientException & ce ){
    exhandler( "caught in put", __FILE__, __LINE__, ns.c_str() );
  } catch (...){
    exhandler( "caught in put", __FILE__, __LINE__, ns.c_str() );
  }
  sk_destroy_val(&pval);
}

void
PutGet::put(string k, string v, SKPutOptions* pPutOpts) {
  SKVal* pval = sk_create_val();
  if(!v.empty()) {
    sk_set_val(pval, v.length(), (void *)v.data());
  }

  try {
    Util::HighResolutionClock log("PutGet-put-sync-putOpts");
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

string
PutGet::get(string k) {
  SKVal* pval = NULL;
  string val;
  try {
    Util::HighResolutionClock log("PutGet-get-sync");
    pval = snsp->get(&k);
  } catch (SKClientException & ce ){
    exhandler( "caught in get", __FILE__, __LINE__, ns.c_str() );
  } catch (...){
    exhandler( "caught in get", __FILE__, __LINE__, ns.c_str() );
  }
  if( pval ){
    val = (char*) (pval->m_pVal);
    sk_destroy_val( &pval );
  } else {
    cout << "\"get\" namespace " << ns << ", key " << k << ", missing value" << endl;
  }
  return val;
}

SKGetOptions*
PutGet::getGetOptions() {
  pGetOpt = GetOpts::getGetOptions(snsp, retrievalType, valueVersion);
  applyGetOptsOnNSPOptions(pGetOpt);
  return pGetOpt;
}

SKNamespacePerspectiveOptions*
PutGet::applyGetOptsOnNSPOptions(SKGetOptions* gOpts) {
  pNspOptions = pNspOptions->defaultGetOptions(gOpts);
  return pNspOptions;
}

string
PutGet::get(string k, SKGetOptions* getOpts) {
  SKStoredValue* pStoredVal = NULL;
  string val;
  try {
    Util::HighResolutionClock log("PutGet-get-sync-getOpts");
    pStoredVal = snsp->get(&k, getOpts);
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

