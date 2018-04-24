#include "AsyncMultiPutAsyncMultiGet.h"

#include "AsyncNSP.h"
#include "GetOpts.h"
#include "Util.h"

AsyncMultiPutAsyncMultiGet::AsyncMultiPutAsyncMultiGet(const string& gc, const string& h, const string& n, const string& log, int verb, const string& nsOpts, const string& jvmOpts, SKCompression::SKCompression compress, long long int version, SKRetrievalType retrieval, const string& k, const string& v, int num, int tmOut)
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

AsyncMultiPutAsyncMultiGet::~AsyncMultiPutAsyncMultiGet() {
  delete pPutOpt;
  delete ansp;
  delete pGetOpt;
}

const vector<string>&
AsyncMultiPutAsyncMultiGet::getKeys() {
  return keys;
}

const vector<string>&
AsyncMultiPutAsyncMultiGet::getValues() {
  return values;
}

int
AsyncMultiPutAsyncMultiGet::getNumOfKeys() {
  return numOfKeys;
}

const map<string, string>&
AsyncMultiPutAsyncMultiGet::getKeyVals() {
  return keyVals;
}

string
AsyncMultiPutAsyncMultiGet::getCompression() {
  return compress;
}

SKAsyncNSPerspective*
AsyncMultiPutAsyncMultiGet::getAnsp() {
  return ansp;
}

SKGetOptions*
AsyncMultiPutAsyncMultiGet::getGetOpt() {
  return pGetOpt;
}

SKAsyncNSPerspective*
AsyncMultiPutAsyncMultiGet::getAsyncNSPerspective() {
  ansp = AsyncNSP::getAsyncNSPerspective(session, ns, pNspOptions);
  return ansp;
}

void
AsyncMultiPutAsyncMultiGet::put(const vector<string>& ks, const vector<string>& vs) {
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
    fprintf(stdout, "SKPutException in AsyncMultiPutAsyncMultiGet : %s" , pe.what() );
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
AsyncMultiPutAsyncMultiGet::put(const map<string, string>& kvs, SKPutOptions* pPutOpts) {
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
    fprintf(stdout, "SKPutException in AsyncMultiPutAsyncMultiGet : %s" , pe.what() );
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

map<string, string>
AsyncMultiPutAsyncMultiGet::get(const vector<string>& ks) {
  StrVector keys = Util::getStrKeys(ks);

  StrValMap* strValMap = NULL;
  SKAsyncValueRetrieval* pValRetrieval = NULL;
  try {
    pValRetrieval = ansp->get(&keys);
    if(pValRetrieval->getState() == SKOperationState::FAILED) {
      SKFailureCause::SKFailureCause reason = pValRetrieval->getFailureCause();
      cout << "Async MGet " << pValRetrieval << ", failure cause: " << reason << endl;
      exit(1);
    }
    pValRetrieval->waitForCompletion();
    if( pValRetrieval->getState()==SKOperationState::FAILED ) {
      SKFailureCause::SKFailureCause reason = pValRetrieval->getFailureCause();
      cout << "pValRetrieval : " << pValRetrieval << ", FailureCause: " << reason << endl;
      exit(1);
    }
    strValMap = pValRetrieval->getValues();
  } catch (SKClientException & ce ){
    exhandler( "caught in get", __FILE__, __LINE__, ns.c_str() );
  } catch (...){
    exhandler( "caught in get", __FILE__, __LINE__, ns.c_str() );
  }

  return Util::getStrMap(strValMap);
}

SKGetOptions*
AsyncMultiPutAsyncMultiGet::getGetOptions() {
  pGetOpt = GetOpts::getGetOptions(ansp, retrievalType, valueVersion);
  return pGetOpt;
}

map<string, string>
AsyncMultiPutAsyncMultiGet::get(const vector<string>& ks, SKGetOptions* getOpts) {
  StrVector keys = Util::getStrKeys(ks);
  StrSVMap* svMap = NULL;
  SKAsyncRetrieval* pRetrieval = NULL;
  try {
    pRetrieval = ansp->get(&keys, getOpts);
    pRetrieval->waitForCompletion();
    if(pRetrieval->getState() == SKOperationState::FAILED) {
      SKFailureCause::SKFailureCause reason = pRetrieval->getFailureCause();
      cout << "Async MGet, pRetrieval: " << pRetrieval << ", failure cause: " << reason << endl;
      exit(1);
    }
 
    svMap = pRetrieval->getStoredValues();
  } catch (SKClientException & ce ){
    exhandler( "caught in get", __FILE__, __LINE__, ns.c_str() );
  } catch (...){
    exhandler( "caught in get", __FILE__, __LINE__, ns.c_str() );
  }

  return Util::getStrMap(svMap, VALUE_AND_META_DATA);
}

