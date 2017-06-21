#include "AsyncMultiPutAsyncMultiGetWait.h"

#include "AsyncNSP.h"
#include "WaitOpts.h"
#include "Util.h"

AsyncMultiPutAsyncMultiGetWait::AsyncMultiPutAsyncMultiGetWait(const string& gc, const string& h, const string& n, const string& log, int verb, const string& nsOpts, const string& jvmOpts, SKCompression::SKCompression compress, long long int version, SKRetrievalType retrieval, const string& k, const string& v, int num, int tmOut, int thresh)
 : DhtAction(gc, h, n, log, verb, nsOpts, jvmOpts, compress, version, retrieval)
 , keys()
 , values()
 , numOfKeys(num)
 , keyVals()
 , timeout(tmOut)
 , threshold(thresh)
 , ansp()
 , pWaitOpt()
 {
   for(int i = 0; i < num; ++i) {
     string key(string(k).append("_").append(to_string(i)));
     string value(string(v).append("_").append(to_string(i)));
     keys.push_back(key);
     values.push_back(value);
     keyVals[key] = value;
   }
 }

AsyncMultiPutAsyncMultiGetWait::~AsyncMultiPutAsyncMultiGetWait() {
  delete pPutOpt;
  delete ansp;
  delete pWaitOpt;
}

const vector<string>&
AsyncMultiPutAsyncMultiGetWait::getKeys() {
  return keys;
}

const vector<string>&
AsyncMultiPutAsyncMultiGetWait::getValues() {
  return values;
}

int
AsyncMultiPutAsyncMultiGetWait::getNumOfKeys() {
  return numOfKeys;
}

const map<string, string>&
AsyncMultiPutAsyncMultiGetWait::getKeyVals() {
  return keyVals;
}

string
AsyncMultiPutAsyncMultiGetWait::getCompression() {
  return compress;
}

SKAsyncNSPerspective*
AsyncMultiPutAsyncMultiGetWait::getAnsp() {
  return ansp;
}

SKWaitOptions*
AsyncMultiPutAsyncMultiGetWait::getWaitOpt() {
  return pWaitOpt;
}

SKAsyncNSPerspective*
AsyncMultiPutAsyncMultiGetWait::getAsyncNSPerspective() {
  ansp = AsyncNSP::getAsyncNSPerspective(session, ns, pNspOptions);
  return ansp;
}

void
AsyncMultiPutAsyncMultiGetWait::put(const vector<string>& ks, const vector<string>& vs) {
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
    fprintf(stdout, "SKPutException in AsyncMultiPutAsyncMultiGetWait : %s" , pe.what() );
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
AsyncMultiPutAsyncMultiGetWait::put(const map<string, string>& kvs, SKPutOptions* pPutOpts) {
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
    fprintf(stdout, "SKPutException in AsyncMultiPutAsyncMultiGetWait : %s" , pe.what() );
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
AsyncMultiPutAsyncMultiGetWait::waitFor(const vector<string>& ks) {
  StrVector keys = Util::getStrKeys(ks);

  StrValMap* strValMap = NULL;
  SKAsyncValueRetrieval* pValRetrieval = NULL;
  try {
    pValRetrieval = ansp->waitFor(&keys);
    if(pValRetrieval->getState() == SKOperationState::FAILED) {
      SKFailureCause::SKFailureCause reason = pValRetrieval->getFailureCause();
      cout << "Async MWaitFor " << pValRetrieval << ", failure cause: " << reason << endl;
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

SKWaitOptions*
AsyncMultiPutAsyncMultiGetWait::getWaitOptions() {
  pWaitOpt = WaitOpts::getWaitOptions(ansp, retrievalType, valueVersion, timeout, threshold);
  return pWaitOpt;
}

map<string, string>
AsyncMultiPutAsyncMultiGetWait::waitFor(const vector<string>& ks, SKWaitOptions* waitOpt) {
  StrVector keys = Util::getStrKeys(ks);
  StrSVMap* svMap = NULL;
  SKAsyncRetrieval* pRetrieval = NULL;
  try {
    pRetrieval = ansp->waitFor(&keys, waitOpt);
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

  return Util::getStrMap(svMap, SKRetrievalType::VALUE_AND_META_DATA);
}

int
AsyncMultiPutAsyncMultiGetWait::getTimeout() {
    return timeout;
}

int
AsyncMultiPutAsyncMultiGetWait::getThreshold() {
  return threshold;
}
