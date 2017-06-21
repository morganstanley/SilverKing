#ifndef SYNC_NS_PERSPECTIVE_H
#define SYNC_NS_PERSPECTIVE_H

#include "Util.h"

class SyncNSP {
public:
  static SKSyncNSPerspective* getSyncNSPerspective(SKSession* session, string namesp, SKNamespacePerspectiveOptions* pNspOpts);
};

#endif
