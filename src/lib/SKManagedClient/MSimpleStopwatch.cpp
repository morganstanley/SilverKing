#include "StdAfx.h"
#include "MSimpleStopwatch.h"
#include "MRelNanosAbsMillisTimeSource.h"

#include <string>
using namespace std;
#include <stdint.h>
#include <stdlib.h>
#include "SKStopwatchBase.h"
#include "SKSimpleStopwatch.h"
#include "SKRelNanosAbsMillisTimeSource.h"


namespace SKManagedClient {

MSimpleStopwatch::!MSimpleStopwatch()
{
    if(pImpl)
    {
        delete (SKSimpleStopwatch*) pImpl;
        pImpl = NULL;
    }
}

MSimpleStopwatch::~MSimpleStopwatch()
{
    this->!MSimpleStopwatch();
}

MSimpleStopwatch::MSimpleStopwatch()
{
    SKSimpleStopwatch * pStopwatch = new SKSimpleStopwatch();
    pImpl = pStopwatch;
}

MSimpleStopwatch::MSimpleStopwatch(MRelNanosAbsMillisTimeSource ^ relNanosTimeSource)
{
    SKSimpleStopwatch * pStopwatch = new SKSimpleStopwatch((SKRelNanosAbsMillisTimeSource*)(relNanosTimeSource->getPImpl()->pTimeSource));
    pImpl = pStopwatch;
}

SKSimpleStopwatch_M ^ MSimpleStopwatch::getPImpl()
{
    SKSimpleStopwatch_M ^ sw = gcnew SKSimpleStopwatch_M;
    sw->pStopwatch = pImpl;
    return sw;
}

MSimpleStopwatch::MSimpleStopwatch(SKSimpleStopwatch_M ^ stopwatch)
{
    pImpl = stopwatch->pStopwatch;
}


MSimpleStopwatch::MSimpleStopwatch(bool ignore) 
{  
     pImpl = NULL;
}



}

