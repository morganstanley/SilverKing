#ifndef SKSTOPWATCHBASE_H
#define SKSTOPWATCHBASE_H

#include "skbasictypes.h"
#include "SKStopwatch.h"

namespace jace { namespace proxy { namespace com { namespace ms { 
    namespace silverking { namespace time {
	    class StopwatchBase;
} } } } } };
typedef jace::proxy::com::ms::silverking::time::StopwatchBase StopwatchBase;

class SKStopwatchBase : public SKStopwatch
{
public:
	//impl
    SKAPI virtual ~SKStopwatchBase();

	SKStopwatchBase(StopwatchBase * pStopwatchBase = NULL);
};

#endif // SKSTOPWATCHBASE_H
