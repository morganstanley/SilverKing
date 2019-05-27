#include "StdAfx.h"
#include "MRelNanosAbsMillisTimeSource.h"

#include <stdlib.h>
#include "SKRelNanosAbsMillisTimeSource.h"

namespace SKManagedClient {

MRelNanosAbsMillisTimeSource::!MRelNanosAbsMillisTimeSource()
{
    if(pImpl) 
    {
        delete (SKRelNanosAbsMillisTimeSource*)pImpl ; 
        pImpl = NULL;
    }
}

MRelNanosAbsMillisTimeSource::~MRelNanosAbsMillisTimeSource()
{
    this->!MRelNanosAbsMillisTimeSource();
}


MRelNanosAbsMillisTimeSource::MRelNanosAbsMillisTimeSource(SKRelNanosAbsMillisTimeSource_M ^ timeSource)
{
    pImpl = timeSource->pTimeSource;
}

Int64 MRelNanosAbsMillisTimeSource::relTimeNanos()
{
    Int64 relTime = ((SKRelNanosAbsMillisTimeSource*)pImpl)->relTimeNanos();
    return relTime;
}


MRelNanosAbsMillisTimeSource::MRelNanosAbsMillisTimeSource() {  }


}
