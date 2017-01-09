#include "StdAfx.h"
#include "MTimerDrivenTimeSource.h"

#include "SKTimerDrivenTimeSource.h"

namespace SKManagedClient {

MTimerDrivenTimeSource::!MTimerDrivenTimeSource()
{
	if(pImpl)
	{
		delete (SKTimerDrivenTimeSource*)pImpl ; 
		pImpl = NULL;
	}
}

MTimerDrivenTimeSource::~MTimerDrivenTimeSource()
{
	this->!MTimerDrivenTimeSource();
}

MTimerDrivenTimeSource::MTimerDrivenTimeSource()
{
	SKTimerDrivenTimeSource* pts  = new SKTimerDrivenTimeSource();
	pImpl = (void *) pts;
}

MTimerDrivenTimeSource::MTimerDrivenTimeSource(Int64 periodMillis)
{
	SKTimerDrivenTimeSource* pts  = new SKTimerDrivenTimeSource(periodMillis);
	pImpl = (void *) pts;
}

MTimerDrivenTimeSource::MTimerDrivenTimeSource(SKTimerDrivenTimeSource_M ^ timeSourceImpl)
{
	pImpl = timeSourceImpl->pTimeSource;
}

/*
SKTimerDrivenTimeSource_M ^ MTimerDrivenTimeSource::getPImpl()
{
	SKTimerDrivenTimeSource_M ^ ts = gcnew SKTimerDrivenTimeSource_M;
	ts->pTimeSource = pImpl;
	return ts;
}

Int64 MTimerDrivenTimeSource::absTimeMillis()
{
	Int64 absTime = ((SKTimerDrivenTimeSource*)pImpl)->absTimeMillis();
	return absTime;
}

int MTimerDrivenTimeSource::relTimeRemaining(Int64 absDeadlineMillis)
{
	return ((SKTimerDrivenTimeSource*)pImpl)->relTimeRemaining(absDeadlineMillis);
}

Int64 MTimerDrivenTimeSource::relTimeNanos()
{
	Int64 relTime = ((SKTimerDrivenTimeSource*)pImpl)->relTimeNanos();
	return relTime;
}
*/

void MTimerDrivenTimeSource::run()
{
	((SKTimerDrivenTimeSource*)pImpl)->run();
}

void MTimerDrivenTimeSource::stop()
{
	((SKTimerDrivenTimeSource*)pImpl)->stop();
}


}