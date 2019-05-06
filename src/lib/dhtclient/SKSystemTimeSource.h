#ifndef SKSYSTEMTIMESOURCE_H
#define SKSYSTEMTIMESOURCE_H

#include "SKRelNanosAbsMillisTimeSource.h"

namespace jace { namespace proxy { namespace com { namespace ms { 
	namespace silverking {namespace time {
		class SystemTimeSource;
} } } } } }
typedef jace::proxy::com::ms::silverking::time::SystemTimeSource SystemTimeSource;


class SKSystemTimeSource : public SKRelNanosAbsMillisTimeSource
{
public:
    SKAPI virtual ~SKSystemTimeSource();
    SKAPI SKSystemTimeSource();
    SKAPI SKSystemTimeSource(int64_t nanosOriginTime);
	
    SKSystemTimeSource(SystemTimeSource * pSystemTimeSource);
    int64_t absTimeNanos();
    
};

#endif //SKSYSTEMTIMESOURCE_H
