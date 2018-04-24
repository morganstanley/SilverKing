#ifndef ASYNC_PUT_ASYNC_GET_H
#define ASYNC_PUT_ASYNC_GET_H

#include "DhtAction.h"

class AsyncPutAsyncGet : public DhtAction {
public:
  AsyncPutAsyncGet(const string& gc, const string& h, const string& n, const string& log, int verb, const string& nsOpts, const string& jvmOpts, SKCompression::SKCompression compress, long long int ver, SKRetrievalType retrieval, const string& k, const string& v, int tmOut);
  ~AsyncPutAsyncGet();

  SKAsyncNSPerspective* getAsyncNSPerspective();

  void put(string key, string value, SKPutOptions* pPutOptions);
  void put(string key, string value);
  
  SKGetOptions* getGetOptions();
  //SKVal* get(const string& key);
  string get(string key);
  //SKStoredValue* get(string& key, SKGetOptions * getOptions);
  string get(string key, SKGetOptions* getOptions);

  string getKey();
  string getValue();
  string getCompression();
  SKPutOptions* getPutOpt();
  SKAsyncNSPerspective* getAnsp();
  SKGetOptions* getGetOpt();
  SKNamespacePerspectiveOptions* applyGetOptsOnNSPOptions(SKGetOptions* gOpts);

private:
  string key;
  string value;
  int timeout;

  SKPutOptions* pPutOpt;
  SKAsyncNSPerspective* ansp;
  SKGetOptions* pGetOpt;
};

#endif
