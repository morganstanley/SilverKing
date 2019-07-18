#ifndef SKABSNANOSTIMESOURCE_H
#define SKABSNANOSTIMESOURCE_H

#include <stdint.h>
#include <cstddef>
#include "skconstants.h"

class SKAbsNanosTimeSource
{
public:
	SKAPI virtual int64_t getNanosOriginTime();
	SKAPI virtual int64_t absTimeNanos();
	SKAPI virtual int64_t relNanosRemaining(int64_t absDeadlineNanos);
    SKAPI virtual ~SKAbsNanosTimeSource();

    SKAbsNanosTimeSource(void * pAbsNanosTimeSource = NULL);
	void * getPImpl();  //FIXME
protected:
	void * _pImpl;
};

#endif //SKABSNANOSTIMESOURCE_H
