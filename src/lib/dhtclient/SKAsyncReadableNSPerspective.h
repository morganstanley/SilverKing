/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#ifndef SKASYNCREADABLENSPERSPECTIVE_H
#define SKASYNCREADABLENSPERSPECTIVE_H

#include "skconstants.h"
#include "skbasictypes.h"
#include "skcontainers.h"
#include "SKBaseNSPerspective.h"

class SKAsyncRetrieval;
class SKAsyncValueRetrieval;
class SKAsyncSingleValueRetrieval;
class SKGetOptions;
class SKWaitOptions;

class SKAsyncReadableNSPerspective : virtual public SKBaseNSPerspective
{
public:
  SKAPI virtual ~SKAsyncReadableNSPerspective(){};

  SKAPI virtual SKAsyncValueRetrieval * get(SKVector<std::string> const * dhtKeys)=0;
  SKAPI virtual SKAsyncRetrieval * get(SKVector<std::string> const * dhtKeys, SKGetOptions * getOptions)=0;
  SKAPI virtual SKAsyncRetrieval * get(const char * key, SKGetOptions * getOptions)=0;
  SKAPI virtual SKAsyncSingleValueRetrieval * get(const char * key)=0;
  SKAPI virtual SKAsyncRetrieval * get(string * key, SKGetOptions * getOptions)=0;
  SKAPI virtual SKAsyncSingleValueRetrieval * get(string * key)=0;
  
  SKAPI virtual SKAsyncValueRetrieval * waitFor(SKVector<std::string> const * dhtKeys)=0;
  SKAPI virtual SKAsyncRetrieval * waitFor(SKVector<std::string> const * dhtKeys, SKWaitOptions * waitOptions)=0;
  SKAPI virtual SKAsyncRetrieval * waitFor(const char * key, SKWaitOptions * waitOptions)=0;
  SKAPI virtual SKAsyncSingleValueRetrieval * waitFor(const char * key)=0;
  SKAPI virtual SKAsyncRetrieval * waitFor(string * key, SKWaitOptions * waitOptions)=0;
  SKAPI virtual SKAsyncSingleValueRetrieval * waitFor(string * key)=0;
};

#endif  //SKASYNCREADABLENSPERSPECTIVE_H
