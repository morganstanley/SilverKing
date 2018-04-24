#include "MultiPutMultiGetMeta.h"

#include "SyncNSP.h"
#include "GetOpts.h"
#include "Util.h"

MultiPutMultiGetMeta::MultiPutMultiGetMeta(const string& gc, const string& h, const string& n, const string& log, int verb, const string& nsOpts, const string& jvmOpts, SKCompression::SKCompression compress, long long int version, SKRetrievalType retrieval, const string& k, const string& v, int num)
 : DhtAction(gc, h, n, log, verb, nsOpts, jvmOpts, compress, version, retrieval)
 , keys()
 , values()
 , numOfKeys(num)
 , keyVals()
 , snsp()
 , pGetOpt()
 {
   for(int i = 0; i < num; ++i) {
     string key(string(k).append("_").append(to_string(i)));
     string value(string(v).append("_").append(to_string(i)));
     keys.push_back(key);
     values.push_back(value);
     keyVals[key] = value;
   }
 }

MultiPutMultiGetMeta::~MultiPutMultiGetMeta() {
  delete pPutOpt;
  delete snsp;
  delete pGetOpt;
}

const vector<string>&
MultiPutMultiGetMeta::getKeys() {
  return keys;
}

const vector<string>&
MultiPutMultiGetMeta::getValues() {
  return values;
}

int
MultiPutMultiGetMeta::getNumOfKeys() {
  return numOfKeys;
}

const map<string, string>&
MultiPutMultiGetMeta::getKeyVals() {
  return keyVals;
}

string
MultiPutMultiGetMeta::getCompression() {
  return compress;
}

SKSyncNSPerspective*
MultiPutMultiGetMeta::getSnsp() {
  return snsp;
}

SKGetOptions*
MultiPutMultiGetMeta::getMetaGetOpt() {
  return pGetOpt;
}

SKSyncNSPerspective*
MultiPutMultiGetMeta::getSyncNSPerspective() {
  snsp = SyncNSP::getSyncNSPerspective(session, ns, pNspOptions);
  return snsp;
}

void
MultiPutMultiGetMeta::put(const vector<string>& ks, const vector<string>& vs) {
  StrValMap vals = Util::getStrValMap(ks, vs);

  try {
    snsp->put(&vals);
  } catch (SKPutException & pe ){
    fprintf(stdout, "SKPutException in MultiPutMultiGetMeta : %s" , pe.what() );
  } catch (SKClientException & ce ){
    exhandler( "caught in put", __FILE__, __LINE__, ns.c_str() );
  } catch (...){
    exhandler( "caught in put", __FILE__, __LINE__, ns.c_str() );
  }
  StrValMap::const_iterator cit ;
  for(cit = vals.begin() ; cit != vals.end(); cit++ ){
    SKVal * pVal = cit->second;
    sk_destroy_val(&pVal);
  }
  vals.clear();
}

void
MultiPutMultiGetMeta::put(const map<string, string>& kvs, SKPutOptions* pPutOpts) {
  StrValMap vals;
  Util::getKeyValues(vals, kvs);

  try {
    snsp->put(&vals, pPutOpts);
  } catch (SKPutException & pe ){
    fprintf(stdout, "SKPutException in MultiPutMultiGetMeta : %s" , pe.what() );
  } catch (SKClientException & ce ){
    exhandler( "caught in put", __FILE__, __LINE__, ns.c_str() );
  } catch (...){
    exhandler( "caught in put", __FILE__, __LINE__, ns.c_str() );
  }
  StrValMap::const_iterator cit ;
  for(cit = vals.begin() ; cit != vals.end(); cit++ ){
    SKVal * pVal = cit->second;
    sk_destroy_val(&pVal);
  }
  vals.clear();
}

SKGetOptions*
MultiPutMultiGetMeta::getMetaGetOptions() {
  pGetOpt = GetOpts::getMetaGetOptions(snsp, retrievalType, valueVersion);
  return pGetOpt;
}

map<string, string>
MultiPutMultiGetMeta::getMeta(const vector<string>& ks, SKGetOptions* getOpts) {
  StrVector keys = Util::getStrKeys(ks);
  StrSVMap* svMap = NULL;
  try {
    svMap = snsp->get(&keys, getOpts);
  } catch (SKClientException & ce ){
    exhandler( "caught in getMeta", __FILE__, __LINE__, ns.c_str() );
  } catch (...){
    exhandler( "caught in getMeta", __FILE__, __LINE__, ns.c_str() );
  }

  return Util::getStrMap(svMap, META_DATA);
}

