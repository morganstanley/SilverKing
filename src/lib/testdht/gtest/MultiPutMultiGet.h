#ifndef MULTI_PUT_MULTI_GET_H
#define MULTI_PUT_MULTI_GET_H

#include "DhtAction.h"

class MultiPutMultiGet : public DhtAction {
public:
  MultiPutMultiGet(const string& gc, const string& h, const string& n, const string& log, int verb, const string& nsOpts, const string& jvmOpts, SKCompression::SKCompression compress, long long int version, SKRetrievalType retrieval, const string& k, const string& v, int num);
  ~MultiPutMultiGet();

  SKSyncNSPerspective* getSyncNSPerspective();

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
  SKSyncNSPerspective* getSnsp();
  SKGetOptions* getGetOpt();

private:
  vector<string> keys;
  vector<string> values;
  int numOfKeys;
  map<string, string> keyVals; // local map used for comparision with the key/value stored in server

  SKSyncNSPerspective* snsp;
  SKGetOptions* pGetOpt;

  StrVector getStrKeys(vector<string>& ks);
};

#endif
