#ifndef PUT_GET_H
#define PUT_GET_H

#include "DhtAction.h"

class PutGet : public DhtAction {
public:
  PutGet(const string& gc, const string& h, const string& n, const string& log, int verb, const string& nsOpts, const string& jvmOpts, SKCompression::SKCompression compress, long long int version, SKRetrievalType retrieval, const string& k, const string& v);
  ~PutGet();

//  SKPutOptions* getPutOptions();
  SKSyncNSPerspective* getSyncNSPerspective();

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
//  SKPutOptions* getPutOpt();
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
