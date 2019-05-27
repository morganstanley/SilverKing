#ifndef SKSTOPWATCH_H
#define SKSTOPWATCH_H

#include <stdint.h>
#include <string>
using std::string;
#include "skconstants.h"

namespace jace { namespace proxy { namespace com { namespace ms { 
    namespace silverking { namespace time {
        class Stopwatch;
    } } } } } };
typedef jace::proxy::com::ms::silverking::time::Stopwatch Stopwatch;

typedef enum StopwatchState_t { 
    SW_RUNNING, SW_STOPPED 
} StopwatchState;

class SKStopwatch 
{
public:

    // control
    SKAPI void start();
    SKAPI void stop();
    SKAPI void reset();

    // elapsed - The stopwatch Must be stopped when calling these method!
    SKAPI int64_t getElapsedNanos();
    SKAPI int64_t getElapsedMillisLong();
    SKAPI int getElapsedMillis();
    SKAPI double getElapsedSeconds();
    // split
    SKAPI int64_t getSplitNanos();
    SKAPI int64_t getSplitMillisLong();
    SKAPI int getSplitMillis();
    SKAPI double getSplitSeconds();
    // misc.
    SKAPI string getName();
    SKAPI StopwatchState getState();
    SKAPI string toStringElapsed();
    SKAPI string toStringSplit();
    SKAPI string toString();

    //impl
    SKAPI virtual ~SKStopwatch();

protected:
    SKStopwatch(Stopwatch * pStopwatch = NULL);
    Stopwatch * getPImpl();

    Stopwatch * pImpl;
};

#endif // SKSTOPWATCH_H
