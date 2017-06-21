#ifndef MULTI_PUT_MULTI_GET_WAIT_H
#define MULTI_PUT_MULTI_GET_WAIT_H

#include "DhtAction.h"

class MultiPutMultiGetWait : public DhtAction {
public:
   MultiPutMultiGetWait(const string& gc, const string& h, const string& n, const string& log, int verb, const string& nsOpts, const string& jvmOpts, SKCompression::SKCompression compress, long long int version, SKRetrievalType retrieval, const string& k, const string& v, int num, int tmOut, int thresh);
  ~MultiPutMultiGetWait();

  SKSyncNSPerspective* getSyncNSPerspective();

  void put(const vector<string>& keys, const vector<string>& values);
  void put(const map<string, string>& kvs, SKPutOptions* pPutOptions);
  
  SKWaitOptions* getWaitOptions();
  map<string, string> waitFor(const vector<string>& keys, SKWaitOptions* waitOpts);

  const vector<string>& getKeys();
  const vector<string>& getValues();
  const map<string, string>& getKeyVals();
  int getNumOfKeys();
  string getCompression();
  SKSyncNSPerspective* getSnsp();
  SKWaitOptions* getWaitOpt();

private:
  vector<string> keys;
  vector<string> values;
  int numOfKeys;
  map<string, string> keyVals; // local map used for comparision with the key/value stored in server

  int timeout;
  int threshold;

  SKSyncNSPerspective* snsp;
  SKWaitOptions* pWaitOpt;

  StrVector getStrKeys(vector<string>& ks);
};

#endif
