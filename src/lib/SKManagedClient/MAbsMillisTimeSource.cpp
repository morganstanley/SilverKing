#include "StdAfx.h"
#include "MAbsMillisTimeSource.h"

#include "SKAbsMillisTimeSource.h"

namespace SKManagedClient {


MAbsMillisTimeSource::!MAbsMillisTimeSource()
{
	if(pImpl)
	{
		delete (SKAbsMillisTimeSource*)pImpl ; 
		pImpl = NULL;
	}
}

MAbsMillisTimeSource::~MAbsMillisTimeSource()
{
	this->!MAbsMillisTimeSource();
}

MAbsMillisTimeSource::MAbsMillisTimeSource() : pImpl(NULL) {  }

MAbsMillisTimeSource::MAbsMillisTimeSource(SKAbsMillisTimeSource_M ^ timeSourceImpl)
{
	pImpl = timeSourceImpl->pTimeSource;
}

SKAbsMillisTimeSource_M ^ MAbsMillisTimeSource::getPImpl()
{
	SKAbsMillisTimeSource_M ^ ts = gcnew SKAbsMillisTimeSource_M;
	ts->pTimeSource = pImpl;
	return ts;
}

Int64 MAbsMillisTimeSource::absTimeMillis()
{
	Int64 absTime = ((SKAbsMillisTimeSource*)pImpl)->absTimeMillis();
	return absTime;
}

int MAbsMillisTimeSource::relMillisRemaining(Int64 absDeadlineMillis)
{
	return ((SKAbsMillisTimeSource*)pImpl)->relMillisRemaining(absDeadlineMillis);
}


}
