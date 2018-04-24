#ifndef PUT_OPTS_H
#define PUT_OPTS_H

#include "Util.h"

class PutOpts {
public:
  static SKPutOptions* getPutOptions(SKNamespacePerspectiveOptions* pNspOptions, string namesp, SKCompression::SKCompression compressType, int valueVersion, SKVal* userData);
};

#endif
