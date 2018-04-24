#include "MultiPutMultiGet.h"

#include "SyncNSP.h"
#include "GetOpts.h"
#include "Util.h"

MultiPutMultiGet::MultiPutMultiGet(const string& gc, const string& h, const string& n, const string& log, int verb, const string& nsOpts, const string& jvmOpts, SKCompression::SKCompression compress, long long int version, SKRetrievalType retrieval, const string& k, const string& v, int num)
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

MultiPutMultiGet::~MultiPutMultiGet() {
  delete pPutOpt;
  delete snsp;
  delete pGetOpt;
}

const vector<string>&
MultiPutMultiGet::getKeys() {
  return keys;
}

const vector<string>&
MultiPutMultiGet::getValues() {
  return values;
}

int
MultiPutMultiGet::getNumOfKeys() {
  return numOfKeys;
}

const map<string, string>&
MultiPutMultiGet::getKeyVals() {
  return keyVals;
}

string
MultiPutMultiGet::getCompression() {
  return compress;
}

SKSyncNSPerspective*
MultiPutMultiGet::getSnsp() {
  return snsp;
}

SKGetOptions*
MultiPutMultiGet::getGetOpt() {
  return pGetOpt;
}

SKSyncNSPerspective*
MultiPutMultiGet::getSyncNSPerspective() {
  snsp = SyncNSP::getSyncNSPerspective(session, ns, pNspOptions);
  return snsp;
}

void
MultiPutMultiGet::put(const vector<string>& ks, const vector<string>& vs) {
  StrValMap vals = Util::getStrValMap(ks, vs);

  try {
    Util::HighResolutionClock log("MultiPutMultiGet-put-sync");
    snsp->put(&vals);
  } catch (SKPutException & pe ){
    fprintf(stdout, "SKPutException in MultiPutMultiGet : %s" , pe.what() );
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
MultiPutMultiGet::put(const map<string, string>& kvs, SKPutOptions* pPutOpts) {
  StrValMap vals;
  Util::getKeyValues(vals, kvs);

  try {
    Util::HighResolutionClock log("MultiPutMultiGet-put-sync-putOpts");
    snsp->put(&vals, pPutOpts);
  } catch (SKPutException & pe ){
    fprintf(stdout, "SKPutException in MultiPutMultiGet : %s" , pe.what() );
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

map<string, string>
MultiPutMultiGet::get(const vector<string>& ks) {
  StrVector keys = Util::getStrKeys(ks);

  StrValMap* strValMap = NULL;
  try {
    Util::HighResolutionClock log("MultiPutMultiGet-get-sync");
    strValMap = snsp->get(&keys);
  } catch (SKClientException & ce ){
    exhandler( "caught in get", __FILE__, __LINE__, ns.c_str() );
  } catch (...){
    exhandler( "caught in get", __FILE__, __LINE__, ns.c_str() );
  }

  return Util::getStrMap(strValMap);
}

SKGetOptions*
MultiPutMultiGet::getGetOptions() {
  pGetOpt = GetOpts::getGetOptions(snsp, retrievalType, valueVersion);
  return pGetOpt;
}

map<string, string>
MultiPutMultiGet::get(const vector<string>& ks, SKGetOptions* getOpts) {
  StrVector keys = Util::getStrKeys(ks);
  StrSVMap* svMap = NULL;
  try {
    Util::HighResolutionClock log("MultiPutMultiGet-get-sync-getOpts");
    svMap = snsp->get(&keys, getOpts);
  } catch (SKClientException & ce ){
    exhandler( "caught in get", __FILE__, __LINE__, ns.c_str() );
  } catch (...){
    exhandler( "caught in get", __FILE__, __LINE__, ns.c_str() );
  }

  return Util::getStrMap(svMap, VALUE_AND_META_DATA);
}

