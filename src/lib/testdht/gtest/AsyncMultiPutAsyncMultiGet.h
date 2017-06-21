#ifndef ASYNC_MULTI_PUT_ASYNC_MULTI_GET_H
#define ASYNC_MULTI_PUT_ASYNC_MULTI_GET_H

#include "DhtAction.h"

class AsyncMultiPutAsyncMultiGet : public DhtAction {
public:
  AsyncMultiPutAsyncMultiGet(const string& gc, const string& h, const string& n, const string& log, int verb, const string& nsOpts, const string& jvmOpts, SKCompression::SKCompression compress, long long int ver, SKRetrievalType retrieval, const string& k, const string& v, int num, int tmOut);
  ~AsyncMultiPutAsyncMultiGet();

  SKAsyncNSPerspective* getAsyncNSPerspective();

  void put(const vector<string>& keys, const vector<string>& values);
  void put(const map<string, string>& kvs, SKPutOptions* pPutOptions);
  
  SKGetOptions* getGetOptions();
  map<string, string> get(const vector<string>& keys);
  map<string, string> get(const vector<string>& keys, SKGetOptions* getOptions);

  const vector<string>& getKeys();
  const vector<string>& getValues();
  const map<string, string>& getKeyVals();
  int getNumOfKeys();
  string getCompression();
  SKAsyncNSPerspective* getAnsp();
  SKGetOptions* getGetOpt();

private:
  vector<string> keys;
  vector<string> values;
  int numOfKeys;
  map<string, string> keyVals; // local map used for comparision with the key/value stored in server

  int timeout;

  SKAsyncNSPerspective* ansp;
  SKGetOptions* pGetOpt;

  StrVector getStrKeys(vector<string>& ks);
};

#endif
