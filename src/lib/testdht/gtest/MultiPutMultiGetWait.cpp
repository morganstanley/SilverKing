#include "MultiPutMultiGetWait.h"

#include "SyncNSP.h"
#include "WaitOpts.h"
#include "Util.h"

MultiPutMultiGetWait::MultiPutMultiGetWait(const string& gc, const string& h, const string& n, const string& log, int verb, const string& nsOpts, const string& jvmOpts, SKCompression::SKCompression compress, long long int version, SKRetrievalType retrieval, const string& k, const string& v, int num, int tmOut, int thresh)
 : DhtAction(gc, h, n, log, verb, nsOpts, jvmOpts, compress, version, retrieval)
 , keys()
 , values()
 , numOfKeys(num)
 , keyVals()
 , timeout(tmOut)
 , threshold(thresh)
 , snsp()
 , pWaitOpt()
 {
   for(int i = 0; i < num; ++i) {
     string   key(string(k).append("_").append(to_string(i)));
     string value(string(v).append("_").append(to_string(i)));
       keys.push_back(key);
     values.push_back(value);
     keyVals[key] = value;
   }
 }

MultiPutMultiGetWait::~MultiPutMultiGetWait() {
  delete pPutOpt;
  delete snsp;
  delete pWaitOpt;
}

const vector<string>&
MultiPutMultiGetWait::getKeys() {
  return keys;
}

const vector<string>&
MultiPutMultiGetWait::getValues() {
  return values;
}

int
MultiPutMultiGetWait::getNumOfKeys() {
  return numOfKeys;
}

const map<string, string>&
MultiPutMultiGetWait::getKeyVals() {
  return keyVals;
}

string
MultiPutMultiGetWait::getCompression() {
  return compress;
}

SKSyncNSPerspective*
MultiPutMultiGetWait::getSnsp() {
  return snsp;
}

SKWaitOptions*
MultiPutMultiGetWait::getWaitOpt() {
  return pWaitOpt;
}

SKSyncNSPerspective*
MultiPutMultiGetWait::getSyncNSPerspective() {
  snsp = SyncNSP::getSyncNSPerspective(session, ns, pNspOptions);
  return snsp;
}

void
MultiPutMultiGetWait::put(const vector<string>& ks, const vector<string>& vs) {
  StrValMap vals = Util::getStrValMap(ks, vs);

  try {
    snsp->put(&vals);
  } catch (SKPutException & pe ){
    fprintf(stdout, "SKPutException in MultiPutMultiGetWait : %s" , pe.what() );
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
MultiPutMultiGetWait::put(const map<string, string>& kvs, SKPutOptions* pPutOpts) {
  StrValMap vals;
  Util::getKeyValues(vals, kvs);

  try {
    snsp->put(&vals, pPutOpts);
  } catch (SKPutException & pe ){
    fprintf(stdout, "SKPutException in MultiPutMultiGetWait : %s" , pe.what() );
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

SKWaitOptions*
MultiPutMultiGetWait::getWaitOptions() {
  pWaitOpt = WaitOpts::getWaitOptions(snsp, retrievalType, valueVersion, timeout, threshold);
  return pWaitOpt;
}

map<string, string>
MultiPutMultiGetWait::waitFor(const vector<string>& ks, SKWaitOptions* opt) {
  StrVector keys = Util::getStrKeys(ks);
  StrSVMap* svMap = NULL;
  try {
    svMap = snsp->waitFor(&keys, opt);
  } catch (SKClientException & ce ){
    exhandler( "caught in get", __FILE__, __LINE__, ns.c_str() );
  } catch (...){
    exhandler( "caught in get", __FILE__, __LINE__, ns.c_str() );
  }

  return Util::getStrMap(svMap);
}

