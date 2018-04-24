#ifndef NAMESPACE_HANDLING_H
#define NAMESPACE_HANDLING_H

#include "DhtAction.h"

class NamespaceHandling : public DhtAction {
public:
  NamespaceHandling(const string& gc, const string& h, const string& n, const string& log, int verb, const string& nsOpts, const string& jvmOpts, SKCompression::SKCompression compress, long long int version, SKRetrievalType retrieval);
  ~NamespaceHandling();

  SKNamespace* cloneNamespace(SKSession* ssn, const string& parent);
  SKNamespaceOptions* setNamespaceOptions(SKNamespaceOptions* nsOpt, SKStorageType::SKStorageType storageType, SKConsistency consistencyProtocol, SKVersionMode versionMode, int segmentSize);

  SKNamespace* getChildNamespace();
private:
  SKNamespace* pChildNamespace;
};

#endif
