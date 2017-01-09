#ifndef SKRELNANOSTIMESOURCE_H
#define SKRELNANOSTIMESOURCE_H

#include <stdint.h>
#include <cstddef>
#include "skconstants.h"

class SKRelNanosTimeSource
{
public:
	SKAPI virtual int64_t relTimeNanos();
    SKAPI virtual ~SKRelNanosTimeSource();

    SKRelNanosTimeSource(void * pRelNanosTimeSource = NULL);
	void * getPImpl();  //FIXME
protected:
	void * pImpl;
};

#endif //SKRELNANOSTIMESOURCE_H
