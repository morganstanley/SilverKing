#ifndef DHT_ACTION_H
#define DHT_ACTION_H

#include "Util.h"

class DhtAction {
public:
  DhtAction(const string& gc, const string& h, const string& n, const string& log, int verb, const string& nsOpts, const string& jvmOpts, SKCompression::SKCompression compress, long long int version, SKRetrievalType retrieval);
  virtual ~DhtAction();

  bool initClient();
  SKClient* getClient();
  SKGridConfiguration* getGridConfiguration();
  SKClientDHTConfiguration* getClientDHTConfiguration();
  SKSessionOptions* getSessionOptions();
  SKSession* openSession();
  SKSession* getSession();
  SKNamespacePerspectiveOptions* getNSPOptions();
  SKNamespacePerspectiveOptions* applyPutOptsOnNSPOptions(SKPutOptions* pOpts);
  SKNamespaceOptions* getNamespaceOpts(); 
  SKNamespaceOptions* getNamespaceOptions();
  SKNamespaceOptions* getNamespaceOptions(const string& optStr);
  string getNamespaceOptStr();
  string getNamespaceName();
  SKNamespace* getNamespace();
  SKNamespace* getNamespace(const string& nmspName, SKNamespaceOptions* nsOpt);
//  SKNamespace* cloneNamespace(SKSession* ssn, const string& parentNs, const string& child);
//  SKNamespace* cloneNamespace(SKSession* ssn, const string& parentNs, const string& child, long ver);
//  SKNamespaceOptions* setNamespaceOptions(SKNamespaceOptions* nsOpt, SKStorageType::SKStorageType strTyp, SKConsistency cnstPrtcl, SKVersionMode versnMd, int segmtSize, bool link);
  SKPutOptions* getPutOptions();

  SKPutOptions* getPutOpt();
  long long int getValueVersion();

  bool isCompressSet();
  bool isValueVersionSet();
//private:
protected:
  string gcName;
  string host; // server name
  string action;
  string ns; // SilverKing namespace
  string logfile;
  int verbose;
  string nsOptions; // namespace options argument passed in
  string jvmOptions;
  SKCompression::SKCompression compressType;
  long long int valueVersion;
  SKRetrievalType retrievalType;

  bool clientInited;
  SKClient* client;
  SKGridConfiguration* pGC;
  SKClientDHTConfiguration* pCdc;
  SKSessionOptions* sessionOption;
  SKSession* session;
  LoggingLevel logLevel;
  SKNamespaceOptions* pNSO;
  SKNamespace* pNamespace;
  SKPutOptions* pPutOpt; // this is unrelated with sync or async namespace

  string compress;
  SKNamespacePerspectiveOptions* pNspOptions;
  SKVal* userData;
};

#endif
