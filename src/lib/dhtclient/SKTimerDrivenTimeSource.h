#ifndef SKTIMERDRIVENTIMESOURCE_H
#define SKTIMERDRIVENTIMESOURCE_H

#include "SKRelNanosAbsMillisTimeSource.h"

namespace jace { namespace proxy { namespace com { namespace ms { 
    namespace silverking {namespace time {
        class TimerDrivenTimeSource;
} } } } } }
typedef jace::proxy::com::ms::silverking::time::TimerDrivenTimeSource TimerDrivenTimeSource;
 

class SKTimerDrivenTimeSource : public SKRelNanosAbsMillisTimeSource
{
public:
    SKAPI virtual ~SKTimerDrivenTimeSource();
    SKAPI SKTimerDrivenTimeSource();
    SKAPI SKTimerDrivenTimeSource(int64_t periodMillis);
    //SKAPI SKTimerDrivenTimeSource(Timer timer);  //FIXME: think how to do this?
    //SKAPI SKTimerDrivenTimeSource(Timer timer, int64_t periodMillis); //FIXME
    
    SKTimerDrivenTimeSource(TimerDrivenTimeSource * pTimerDrivenTimeSource);
    
    SKAPI void run();
    SKAPI void stop();
};

#endif //SKTIMERDRIVENTIMESOURCE_H
