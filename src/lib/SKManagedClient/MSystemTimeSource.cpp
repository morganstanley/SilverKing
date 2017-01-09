#include "StdAfx.h"
#include "MSystemTimeSource.h"

#include "SKSystemTimeSource.h"

namespace SKManagedClient {


MSystemTimeSource::!MSystemTimeSource()
{
	if(pImpl)
	{
		delete (SKSystemTimeSource*)pImpl ; 
		pImpl = NULL;
	}
}

MSystemTimeSource::~MSystemTimeSource()
{
	this->!MSystemTimeSource();
}

MSystemTimeSource::MSystemTimeSource()
{
	SKSystemTimeSource* pts  = new SKSystemTimeSource();
	pImpl = (void *) pts;
}

MSystemTimeSource::MSystemTimeSource(SKSystemTimeSource_M ^ timeSourceImpl)
{
	pImpl = timeSourceImpl->pTimeSource;
}

/*
SKSystemTimeSource_M ^ MSystemTimeSource::getPImpl()
{
	SKSystemTimeSource_M ^ ts = gcnew SKSystemTimeSource_M;
	ts->pTimeSource = pImpl;
	return ts;
}

Int64 MSystemTimeSource::absTimeMillis()
{
	Int64 absTime = ((SKSystemTimeSource*)pImpl)->absTimeMillis();
	return absTime;
}

int MSystemTimeSource::relTimeRemaining(Int64 absDeadlineMillis)
{
	return ((SKSystemTimeSource*)pImpl)->relTimeRemaining(absDeadlineMillis);
}

Int64 MSystemTimeSource::relTimeNanos()
{
	Int64 relTime = ((SKSystemTimeSource*)pImpl)->relTimeNanos();
	return relTime;
}
*/

}