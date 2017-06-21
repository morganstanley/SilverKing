#ifndef WAIT_OPTS_H
#define WAIT_OPTS_H

#include "Util.h"

class WaitOpts {
public:
  static SKWaitOptions* getWaitOptions(SKSyncNSPerspective* snsp,  SKRetrievalType retrievalType, int valueVersion, int timeOut, int threshold);
  static SKWaitOptions* getWaitOptions(SKAsyncNSPerspective* ansp, SKRetrievalType retrievalType, int valueVersion, int timeOut, int threshold);
};

#endif
