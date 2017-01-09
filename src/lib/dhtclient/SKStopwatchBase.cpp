#include <exception>
#include "SKStopwatchBase.h"
#include "jace/proxy/com/ms/silverking/time/StopwatchBase.h"
using jace::proxy::com::ms::silverking::time::StopwatchBase;

//impl
SKStopwatchBase::SKStopwatchBase(StopwatchBase * pStopwatchBase) {
	if(pStopwatchBase)
		pImpl = pStopwatchBase;
} 
	
SKStopwatchBase::~SKStopwatchBase()
{
	if(pImpl!=NULL) {
		StopwatchBase * pStopwatchBase = dynamic_cast<StopwatchBase*>(pImpl);
		delete pStopwatchBase; 
		pImpl = NULL;
	}
}
