#ifndef ASYNC_MULTI_PUT_ASYNC_MULTI_GET_WAIT_H
#define ASYNC_MULTI_PUT_ASYNC_MULTI_GET_WAIT_H

#include "DhtAction.h"

class AsyncMultiPutAsyncMultiGetWait : public DhtAction {
public:
  AsyncMultiPutAsyncMultiGetWait(const string& gc, const string& h, const string& n, const string& log, int verb, const string& nsOpts, const string& jvmOpts, SKCompression::SKCompression compress, long long int version, SKRetrievalType retrieval, const string& k, const string& v, int num, int tmOut, int thresh);
  ~AsyncMultiPutAsyncMultiGetWait();

  SKAsyncNSPerspective* getAsyncNSPerspective();

  void put(const vector<string>& keys, const vector<string>& values);
  void put(const map<string, string>& kvs, SKPutOptions* pPutOptions);
  
  SKWaitOptions* getWaitOptions();
  map<string, string> waitFor(const vector<string>& keys);
  map<string, string> waitFor(const vector<string>& keys, SKWaitOptions* waitOpt);

  const vector<string>& getKeys();
  const vector<string>& getValues();
  const map<string, string>& getKeyVals();
  int getNumOfKeys();
  string getCompression();
  SKAsyncNSPerspective* getAnsp();
  SKWaitOptions* getWaitOpt();

private:
  vector<string> keys;
  vector<string> values;
  int numOfKeys;
  map<string, string> keyVals; // local map used for comparision with the key/value stored in server

  int timeout;
  int threshold;

  SKAsyncNSPerspective* ansp;
  SKWaitOptions* pWaitOpt;

  int getTimeout();
  int getThreshold();
};

#endif
