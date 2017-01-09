#ifndef SKASYNCWRITABLENSPERSPECTIVE_H
#define SKASYNCWRITABLENSPERSPECTIVE_H

#include <string>
using std::string;
#include "skconstants.h"
#include "skbasictypes.h"
#include "skcontainers.h"
#include "SKAsyncInvalidation.h"
#include "SKAsyncPut.h"
#include "SKBaseNSPerspective.h"
#include "SKInvalidationOptions.h"
#include "SKPutOptions.h"

class SKAsyncWritableNSPerspective : virtual public SKBaseNSPerspective
{
public:
  SKAPI virtual ~SKAsyncWritableNSPerspective(){};

  SKAPI virtual SKAsyncPut *put(SKMap<string, SKVal*> const *dhtValues)=0;
  SKAPI virtual SKAsyncPut *put(SKMap<string, SKVal*> const *dhtValues, SKPutOptions *putOptions)=0;
  SKAPI virtual SKAsyncPut *put(const char *key, const SKVal *value, SKPutOptions *putOptions)=0;
  SKAPI virtual SKAsyncPut *put(const char *key, const SKVal *value)=0;
  SKAPI virtual SKAsyncPut *put(string *key, const SKVal *value, SKPutOptions *putOptions)=0;
  SKAPI virtual SKAsyncPut *put(string *key, const SKVal *value)=0;
  
  SKAPI virtual SKAsyncInvalidation *invalidate(SKVector<std::string> const *dhtKeys)=0;
  SKAPI virtual SKAsyncInvalidation *invalidate(SKVector<std::string> const *dhtKeys, SKInvalidationOptions * invalidationOptions)=0;
  SKAPI virtual SKAsyncInvalidation *invalidate(const char * key, SKInvalidationOptions *invalidationOptions)=0;
  SKAPI virtual SKAsyncInvalidation *invalidate(const char * key)=0;
  SKAPI virtual SKAsyncInvalidation *invalidate(string * key, SKInvalidationOptions * invalidationOptions)=0;
  SKAPI virtual SKAsyncInvalidation *invalidate(string * key)=0;  
};


#endif  //SKASYNCWRITABLENSPERSPECTIVE_H
