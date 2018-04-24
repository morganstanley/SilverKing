#ifndef ASYNC_NS_PERSPECTIVE_H
#define ASYNC_NS_PERSPECTIVE_H

#include "Util.h"

class AsyncNSP {
public:
  static SKAsyncNSPerspective* getAsyncNSPerspective(SKSession* session, string namesp, SKNamespacePerspectiveOptions* pNspOpts);
};

#endif
