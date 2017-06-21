#include "PutGetMeta.h"

#include "PutOpts.h"
#include "SyncNSP.h"
#include "GetOpts.h"

PutGetMeta::PutGetMeta(const string& gc, const string& h, const string& n, const string& log, int verb, const string& nsOpts, const string& jvmOpts, SKCompression::SKCompression compress, long long int version, SKRetrievalType retrieval, const string& k, const string& v)
 : DhtAction(gc, h, n, log, verb, nsOpts, jvmOpts, compress, version, retrieval)
 , key(k)
 , value(v)
 , pPutOpt()
 , snsp()
 , pGetOpt()
 {
 }

PutGetMeta::~PutGetMeta() {
  delete pPutOpt;
  delete snsp;
  delete pGetOpt;
}

string
PutGetMeta::getKey() {
  return key;
}

string
PutGetMeta::getValue() {
  return value;
}

string
PutGetMeta::getCompression() {
  return compress;
}

SKSyncNSPerspective*
PutGetMeta::getSnsp() {
  return snsp;
}

SKGetOptions*
PutGetMeta::getGetOpt() {
  return pGetOpt;
}

SKSyncNSPerspective*
PutGetMeta::getSyncNSPerspective() {
  snsp = SyncNSP::getSyncNSPerspective(session, ns, pNspOptions);
  return snsp;
}

void
PutGetMeta::put(string k, string v) {
  SKVal* pval = sk_create_val();
  if(!v.empty()) {
    sk_set_val(pval, v.length(), (void *)v.data());
  }

  try {
    snsp->put(&k, pval);
  } catch (SKPutException & pe ){
    fprintf(stdout, "SKPutException in PutGetMeta : %s" , pe.what() );
  } catch (SKClientException & ce ){
    exhandler( "caught in put", __FILE__, __LINE__, ns.c_str() );
  } catch (...){
    exhandler( "caught in put", __FILE__, __LINE__, ns.c_str() );
  }
  sk_destroy_val(&pval);
}

void
PutGetMeta::put(string k, string v, SKPutOptions* pPutOpts) {
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

SKGetOptions*
PutGetMeta::getMetaGetOptions() {
  pGetOpt = GetOpts::getMetaGetOptions(snsp, retrievalType, valueVersion);
  applyGetOptsOnNSPOptions(pGetOpt);
  return pGetOpt;
}

SKNamespacePerspectiveOptions*
PutGetMeta::applyGetOptsOnNSPOptions(SKGetOptions* gOpts) {
  pNspOptions = pNspOptions->defaultGetOptions(gOpts);
  return pNspOptions;
}

string
PutGetMeta::getMeta(string k, SKGetOptions* getOpts) {
  SKStoredValue* pStoredVal = NULL;
  string val;
  try {
    pStoredVal = snsp->get(&k, getOpts);
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

