/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#ifndef SKSYNCREADABLENSPERSPECTIVE_H
#define SKSYNCREADABLENSPERSPECTIVE_H

#include "SKBaseNSPerspective.h"
#include "skcontainers.h"
#include <string>
using std::string;

struct SKVal;
class SKStoredValue;
class SKGetOptions;
class SKWaitOptions;

class SKSyncReadableNSPerspective : virtual public SKBaseNSPerspective
{
public:
	SKAPI virtual ~SKSyncReadableNSPerspective(){};

	// get - do not wait for key-value pairs to exist
	SKAPI virtual SKMap<string, SKStoredValue*> * get(SKVector<string> const * keys, SKGetOptions * getOptions)=0;
	SKAPI virtual SKMap<string, SKVal*> * get(SKVector<string> const * keys)=0;
	SKAPI virtual SKStoredValue * get(string * key, SKGetOptions * getOptions)=0;
	SKAPI virtual SKVal * get(string * key)=0;
	SKAPI virtual SKStoredValue * get(const char * key, SKGetOptions * getOptions)=0;
	SKAPI virtual SKVal * get(const char * key)=0;
		
	// waitFor - wait on non-existent key-value pairs
	SKAPI virtual SKMap<string, SKStoredValue*> * waitFor(SKVector<string> const * keys, SKWaitOptions * waitOptions)=0;
	SKAPI virtual SKMap<string, SKVal*> *  waitFor(SKVector<string> const * keys)=0;
	SKAPI virtual SKStoredValue * waitFor(string * key, SKWaitOptions * waitOptions)=0;
	SKAPI virtual SKVal * waitFor(string * key)=0;
	SKAPI virtual SKStoredValue * waitFor(const char * key, SKWaitOptions * waitOptions)=0;
	SKAPI virtual SKVal * waitFor(const char * key)=0;
 
};


#endif  //SKSYNCREADABLENSPERSPECTIVE_H
