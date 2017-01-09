#include "StdAfx.h"
#include "MConstantAbsMillisTimeSource.h"

#include <stdlib.h>
#include "SKConstantAbsMillisTimeSource.h"

namespace SKManagedClient {

MConstantAbsMillisTimeSource::!MConstantAbsMillisTimeSource()
{
	if(pImpl) 
	{
		delete (SKConstantAbsMillisTimeSource*)pImpl ; 
		pImpl = NULL;
	}
}

MConstantAbsMillisTimeSource::~MConstantAbsMillisTimeSource()
{
	this->!MConstantAbsMillisTimeSource();
}

MConstantAbsMillisTimeSource::MConstantAbsMillisTimeSource(Int64 absMillisTime)
{
	SKConstantAbsMillisTimeSource* pts  = new SKConstantAbsMillisTimeSource(absMillisTime);
	pImpl = (void *) pts;
}

MConstantAbsMillisTimeSource::MConstantAbsMillisTimeSource(SKConstantAbsMillisTimeSource_M ^ timeSource)
{
	pImpl = timeSource->pTimeSource;
}

SKConstantAbsMillisTimeSource_M ^ MConstantAbsMillisTimeSource::getPImpl()
{
	SKConstantAbsMillisTimeSource_M ^ ts = gcnew SKConstantAbsMillisTimeSource_M;
	ts->pTimeSource = pImpl;
	return ts;
}


}
