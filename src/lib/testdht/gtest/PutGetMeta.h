#ifndef PUT_GET_META_H
#define PUT_GET_META_H

#include "DhtAction.h"

class PutGetMeta : public DhtAction {
public:
  PutGetMeta(const string& gc, const string& h, const string& n, const string& log, int verb, const string& nsOpts, const string& jvmOpts, SKCompression::SKCompression compress, long long int version, SKRetrievalType retrieval, const string& k, const string& v);
  ~PutGetMeta();

  SKSyncNSPerspective* getSyncNSPerspective();

  void put(string key, string value, SKPutOptions* pPutOptions);
  void put(string key, string value);
  
  SKGetOptions* getMetaGetOptions();
  string getMeta(string key, SKGetOptions* getOptions);

  string getKey();
  string getValue();
  string getCompression();
  SKSyncNSPerspective* getSnsp();
  SKGetOptions* getGetOpt();
  SKNamespacePerspectiveOptions* applyGetOptsOnNSPOptions(SKGetOptions* gOpts);

private:
  string key;
  string value;

  SKPutOptions* pPutOpt;
  SKSyncNSPerspective* snsp;
  SKGetOptions* pGetOpt;
};

#endif
