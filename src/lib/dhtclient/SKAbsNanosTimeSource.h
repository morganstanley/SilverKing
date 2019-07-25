#ifndef SKABSNANOSTIMESOURCE_H
#define SKABSNANOSTIMESOURCE_H

#include <stdint.h>
#include <cstddef>
#include "skconstants.h"

namespace jace { namespace proxy { namespace com { namespace ms { 
    namespace silverking {namespace time {
        class AbsNanosTimeSource;
} } } } } }
typedef jace::proxy::com::ms::silverking::time::AbsNanosTimeSource AbsNanosTimeSource;


class SKAbsNanosTimeSource
{
public:
	SKAPI int64_t getNanosOriginTime();
	SKAPI int64_t absTimeNanos();
	SKAPI int64_t relNanosRemaining(int64_t absDeadlineNanos);
    SKAPI virtual ~SKAbsNanosTimeSource();

    SKAbsNanosTimeSource(void * pAbsNanosTimeSource = NULL);
	void * getPImpl();  //FIXME
protected:
	void * _pImpl;
};

#endif //SKABSNANOSTIMESOURCE_H
