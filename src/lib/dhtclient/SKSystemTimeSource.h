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
	
    SKSystemTimeSource(SystemTimeSource * pSystemTimeSource);
};

#endif //SKSYSTEMTIMESOURCE_H
