#include "SKTimerDrivenTimeSource.h"

#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using namespace jace;
#include "jace/proxy/com/ms/silverking/time/TimerDrivenTimeSource.h"
using jace::proxy::com::ms::silverking::time::TimerDrivenTimeSource;


SKTimerDrivenTimeSource::SKTimerDrivenTimeSource() 
    : SKRelNanosAbsMillisTimeSource() 
{
    pImpl = new TimerDrivenTimeSource(java_new<TimerDrivenTimeSource>());
}

SKTimerDrivenTimeSource::SKTimerDrivenTimeSource(int64_t periodMillis) 
    : SKRelNanosAbsMillisTimeSource() 
{
    pImpl = new TimerDrivenTimeSource(java_new<TimerDrivenTimeSource>(periodMillis));
}

//SKTimerDrivenTimeSource::SKTimerDrivenTimeSource(Timer timer);
//SKTimerDrivenTimeSource::SKTimerDrivenTimeSource(Timer timer, int64_t periodMillis);


SKTimerDrivenTimeSource::SKTimerDrivenTimeSource(TimerDrivenTimeSource * pTimerDrivenTimeSource) //FIXME: ?
    : SKRelNanosAbsMillisTimeSource()
{ 
    if(pTimerDrivenTimeSource)
        pImpl = pTimerDrivenTimeSource;
}

SKTimerDrivenTimeSource::~SKTimerDrivenTimeSource() {
    if(pImpl) {
        TimerDrivenTimeSource* pTs = dynamic_cast<TimerDrivenTimeSource*>(pImpl);
        delete pTs;
        pImpl = NULL;
    }
};

void SKTimerDrivenTimeSource::run(){
    (dynamic_cast<TimerDrivenTimeSource*>(pImpl))->run();
}

void SKTimerDrivenTimeSource::stop(){
    (dynamic_cast<TimerDrivenTimeSource*>(pImpl))->stop();
}

