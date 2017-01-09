#ifndef SKABSMILLISTIMESOURCE_H
#define SKABSMILLISTIMESOURCE_H

#include <stdint.h>
#include <cstddef>
#include "skconstants.h"

namespace jace { namespace proxy { namespace com { namespace ms { 
	namespace silverking {namespace time {
		class AbsMillisTimeSource;
} } } } } }
typedef jace::proxy::com::ms::silverking::time::AbsMillisTimeSource AbsMillisTimeSource;
 

class SKAbsMillisTimeSource
{
public:
	SKAPI int64_t absTimeMillis();
	SKAPI int relMillisRemaining(int64_t absDeadlineMillis);
    SKAPI virtual ~SKAbsMillisTimeSource();
    SKAPI SKAbsMillisTimeSource(AbsMillisTimeSource * pAbsMillisTimeSource = NULL);

	AbsMillisTimeSource * getPImpl();  //FIXME
protected:
	AbsMillisTimeSource * pImpl;
};

#endif //SKABSMILLISTIMESOURCE_H
