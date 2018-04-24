#ifndef GET_OPTS_H
#define GET_OPTS_H

#include "DhtAction.h"

class GetOpts {
public:
  static SKVersionConstraint* getVersionConstraint(int valueVersion);
  static SKGetOptions*     getGetOptions(SKSyncNSPerspective*  snsp, SKRetrievalType retrievalType, int valueVersion);
  static SKGetOptions* getMetaGetOptions(SKSyncNSPerspective*  snsp, SKRetrievalType retrievalType, int valueVersion);
  static SKGetOptions*     getGetOptions(SKAsyncNSPerspective* ansp, SKRetrievalType retrievalType, int valueVersion);
  static SKGetOptions* getMetaGetOptions(SKAsyncNSPerspective* ansp, SKRetrievalType retrievalType, int valueVersion);
};

#endif
