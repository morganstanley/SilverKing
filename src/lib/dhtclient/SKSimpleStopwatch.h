#ifndef SKSIMPLESTOPWATCH_H
#define SKSIMPLESTOPWATCH_H

#include "skconstants.h"
#include "SKStopwatchBase.h"

class SKRelNanosTimeSource;
class SKRelNanosAbsMillisTimeSource;
namespace jace { namespace proxy { namespace com { namespace ms { 
    namespace silverking { namespace time {
        class SimpleStopwatch;
} } } } } };
typedef jace::proxy::com::ms::silverking::time::SimpleStopwatch SimpleStopwatch;

class SKSimpleStopwatch : public SKStopwatchBase
{
public:
    SKAPI SKSimpleStopwatch();
    SKAPI SKSimpleStopwatch(SKRelNanosAbsMillisTimeSource * relNanosTimeSource);
    SKAPI virtual ~SKSimpleStopwatch();

protected:
    SKSimpleStopwatch(SimpleStopwatch * pSimpleStopwatch);
};

#endif // SKSIMPLESTOPWATCH_H
