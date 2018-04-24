#ifndef ASYNC_PUT_ASYNC_GET_META_H
#define ASYNC_PUT_ASYNC_GET_META_H

#include "DhtAction.h"

class AsyncPutAsyncGetMeta : public DhtAction {
public:
  AsyncPutAsyncGetMeta(const string& gc, const string& h, const string& n, const string& log, int verb, const string& nsOpts, const string& jvmOpts, SKCompression::SKCompression compress, long long int version, SKRetrievalType retrieval, const string& k, const string& v, int tmOut);
  ~AsyncPutAsyncGetMeta();

  SKAsyncNSPerspective* getAsyncNSPerspective();

  void put(string key, string value, SKPutOptions* pPutOptions);
  void put(string key, string value);
  
  SKGetOptions* getMetaGetOptions();
  string getMeta(string key, SKGetOptions* getOptions);

  string getKey();
  string getValue();
  string getCompression();
  SKPutOptions* getPutOpt();
  SKAsyncNSPerspective* getAnsp();
  SKGetOptions* getMetaGetOpt();
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
