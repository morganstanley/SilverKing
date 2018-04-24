#ifndef ASYNC_PUT_ASYNC_GET_WAIT_H
#define ASYNC_PUT_ASYNC_GET_WAIT_H

#include "DhtAction.h"

class AsyncPutAsyncGetWait : public DhtAction {
public:
  AsyncPutAsyncGetWait(const string& gc, const string& h, const string& n, const string& log, int verb, const string& nsOpts, const string& jvmOpts, SKCompression::SKCompression compress, long long int version, SKRetrievalType retrieval, const string& k, const string& v, int tmOut, int thresh);
  ~AsyncPutAsyncGetWait();

  SKAsyncNSPerspective* getAsyncNSPerspective();

  void put(string key, string value, SKPutOptions* pPutOptions);
  void put(string key, string value);
  
  SKWaitOptions* getWaitOptions();
  string waitFor(string key);
  string waitFor(string key, SKWaitOptions* waitOpt);

  string getKey();
  string getValue();
  string getCompression();
  SKAsyncNSPerspective* getAnsp();
  SKWaitOptions* getWaitOpt();

  int getTimeout();
  int getThreshold();

private:
  string key;
  string value;
  int timeout;
  int threshold;

  SKPutOptions* pPutOpt;
  SKAsyncNSPerspective* ansp;
  SKWaitOptions* pWaitOpt;
};

#endif
