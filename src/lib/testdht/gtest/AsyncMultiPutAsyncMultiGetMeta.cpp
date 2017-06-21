#include "AsyncMultiPutAsyncMultiGetMeta.h"

#include "AsyncNSP.h"
#include "GetOpts.h"
#include "Util.h"

AsyncMultiPutAsyncMultiGetMeta::AsyncMultiPutAsyncMultiGetMeta(const string& gc, const string& h, const string& n, const string& log, int verb, const string& nsOpts, const string& jvmOpts, SKCompression::SKCompression compress, long long int version, SKRetrievalType retrieval, const string& k, const string& v, int num, int tmOut)
 : DhtAction(gc, h, n, log, verb, nsOpts, jvmOpts, compress, version, retrieval)
 , keys()
 , values()
 , numOfKeys(num)
 , keyVals()
 , timeout(tmOut)
 , ansp()
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

AsyncMultiPutAsyncMultiGetMeta::~AsyncMultiPutAsyncMultiGetMeta() {
  delete pPutOpt;
  delete ansp;
  delete pGetOpt;
}

const vector<string>&
AsyncMultiPutAsyncMultiGetMeta::getKeys() {
  return keys;
}

const vector<string>&
AsyncMultiPutAsyncMultiGetMeta::getValues() {
  return values;
}

int
AsyncMultiPutAsyncMultiGetMeta::getNumOfKeys() {
  return numOfKeys;
}

const map<string, string>&
AsyncMultiPutAsyncMultiGetMeta::getKeyVals() {
  return keyVals;
}

string
AsyncMultiPutAsyncMultiGetMeta::getCompression() {
  return compress;
}

SKAsyncNSPerspective*
AsyncMultiPutAsyncMultiGetMeta::getAnsp() {
  return ansp;
}

SKGetOptions*
AsyncMultiPutAsyncMultiGetMeta::getMetaGetOpt() {
  return pGetOpt;
}

SKAsyncNSPerspective*
AsyncMultiPutAsyncMultiGetMeta::getAsyncNSPerspective() {
  ansp = AsyncNSP::getAsyncNSPerspective(session, ns, pNspOptions);
  return ansp;
}

void
AsyncMultiPutAsyncMultiGetMeta::put(const vector<string>& ks, const vector<string>& vs) {
  StrValMap vals = Util::getStrValMap(ks, vs);
  SKAsyncPut* asyncPut = NULL;

  try {
    asyncPut = ansp->put(&vals);
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
    fprintf(stdout, "SKPutException in AsyncMultiPutAsyncMultiGetMeta : %s" , pe.what() );
  } catch (SKClientException & ce ){
    exhandler( "caught in put", __FILE__, __LINE__, ns.c_str() );
  } catch (...){
    exhandler( "caught in put", __FILE__, __LINE__, ns.c_str() );
  }
  asyncPut->close();
  delete asyncPut;
  StrValMap::const_iterator cit ;
  for(cit = vals.begin(); cit != vals.end(); cit++) {
    SKVal * pVal = cit->second;
    sk_destroy_val(&pVal);
  }
  vals.clear();
}

void
AsyncMultiPutAsyncMultiGetMeta::put(const map<string, string>& kvs, SKPutOptions* pPutOpts) {
  StrValMap vals;
  Util::getKeyValues(vals, kvs);
  SKAsyncPut* asyncPut = NULL;

  try {
    ansp->put(&vals, pPutOpts);
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
    fprintf(stdout, "SKPutException in AsyncMultiPutAsyncMultiGetMeta : %s" , pe.what() );
  } catch (SKClientException & ce ){
    exhandler( "caught in put", __FILE__, __LINE__, ns.c_str() );
  } catch (...){
    exhandler( "caught in put", __FILE__, __LINE__, ns.c_str() );
  }
  asyncPut->close();
  delete asyncPut;
  StrValMap::const_iterator cit ;
  for(cit = vals.begin() ; cit != vals.end(); cit++ ){
    SKVal * pVal = cit->second;
    sk_destroy_val(&pVal);
  }
  vals.clear();
}

SKGetOptions*
AsyncMultiPutAsyncMultiGetMeta::getMetaGetOptions() {
  pGetOpt = GetOpts::getMetaGetOptions(ansp, retrievalType, valueVersion);
  return pGetOpt;
}

map<string, string>
AsyncMultiPutAsyncMultiGetMeta::getMeta(const vector<string>& ks, SKGetOptions* getOpts) {
  StrVector keys = Util::getStrKeys(ks);
  StrSVMap* svMap = NULL;
  SKAsyncRetrieval* pRetrieval = NULL;
  try {
    pRetrieval = ansp->get(&keys, getOpts);
    pRetrieval->waitForCompletion();
    if(pRetrieval->getState() == SKOperationState::FAILED) {
      SKFailureCause::SKFailureCause reason = pRetrieval->getFailureCause();
      cout << "Async MGetMeta, pRetrieval: " << pRetrieval << ", failure cause: " << reason << endl;
      exit(1);
    }
 
    svMap = pRetrieval->getStoredValues();
  } catch (SKClientException & ce ){
    exhandler( "caught in get", __FILE__, __LINE__, ns.c_str() );
  } catch (...){
    exhandler( "caught in get", __FILE__, __LINE__, ns.c_str() );
  }

  return Util::getStrMap(svMap, META_DATA);
}

