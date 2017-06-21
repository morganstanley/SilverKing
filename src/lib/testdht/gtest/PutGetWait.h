#ifndef PUT_GET_WAIT_H
#define PUT_GET_WAIT_H

#include "DhtAction.h"

class PutGetWait : public DhtAction {
public:
  PutGetWait(const string& gc, const string& h, const string& n, const string& log, int verb, const string& nsOpts, const string& jvmOpts, SKCompression::SKCompression compress, long long int version, SKRetrievalType retrieval, const string& k, const string& v, int tmOut, int thresh);
  ~PutGetWait();

  SKSyncNSPerspective* getSyncNSPerspective();

  void put(string key, string value, SKPutOptions* pPutOptions);
  void put(string key, string value);
  
  SKWaitOptions* getWaitOptions();
  string waitFor(string key, SKWaitOptions* waitOpt);

  string getKey();
  string getValue();
  string getCompression();
  SKSyncNSPerspective* getSnsp();
  SKWaitOptions* getWaitOpt();

private:
  string key;
  string value;
  int timeout;
  int threshold;

  SKPutOptions* pPutOpt;
  SKSyncNSPerspective* snsp;
  SKWaitOptions* waitOpt;
};

#endif
