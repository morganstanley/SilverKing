/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#ifndef SKSYNCWRITABLENSPERSPECTIVE_H
#define SKSYNCWRITABLENSPERSPECTIVE_H

#include <stdint.h>
#include "SKBaseNSPerspective.h"
#include "skconstants.h"
#include "skcontainers.h"

class SKPutOptions;

class SKSyncWritableNSPerspective : virtual public SKBaseNSPerspective
{
public:
	SKAPI virtual ~SKSyncWritableNSPerspective(){};

	SKAPI virtual void put( SKMap<string, SKVal*> const * dhtValues)=0;
	SKAPI virtual void put( SKMap<string, SKVal*> const * dhtValues, SKPutOptions * pPutOptions)=0;
	SKAPI virtual void put(string * key, SKVal* value, SKPutOptions * pPutOptions)=0;
	SKAPI virtual void put(string * key, SKVal* value)=0;
	SKAPI virtual void put(const char * key, SKVal* value, SKPutOptions * pPutOptions)=0;
	SKAPI virtual void put(const char * key, SKVal* value)=0;
};


#endif  //SKSYNCWRITABLENSPERSPECTIVE_H
