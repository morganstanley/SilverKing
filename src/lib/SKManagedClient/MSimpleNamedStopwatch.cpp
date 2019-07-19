#include "StdAfx.h"
#include "MSimpleNamedStopwatch.h"
#include "MRelNanosAbsMillisTimeSource.h"

#include <string>
using namespace std;
#include <stdint.h>
#include <stdlib.h>
#include "SKSimpleNamedStopwatch.h"
#include "SKRelNanosAbsMillisTimeSource.h"

using namespace System;
using namespace System::Runtime::InteropServices;

namespace SKManagedClient {

MSimpleNamedStopwatch::!MSimpleNamedStopwatch()
{
    if(pImpl)
    {
        delete (SKSimpleNamedStopwatch*)pImpl;
        pImpl = NULL;
    }
}

MSimpleNamedStopwatch::~MSimpleNamedStopwatch()
{
    this->!MSimpleNamedStopwatch();
}

MSimpleNamedStopwatch::MSimpleNamedStopwatch() : MSimpleStopwatch(true)
{
    SKSimpleNamedStopwatch * pStopwatch = new SKSimpleNamedStopwatch();
    pImpl = pStopwatch;
}

MSimpleNamedStopwatch::MSimpleNamedStopwatch(String ^ name) : MSimpleStopwatch(true)
{
    SKSimpleNamedStopwatch * pStopwatch = new SKSimpleNamedStopwatch(
        (char *) System::Runtime::InteropServices::Marshal::StringToHGlobalAnsi(name).ToPointer()
    );
    pImpl = pStopwatch;
}

MSimpleNamedStopwatch::MSimpleNamedStopwatch(MRelNanosAbsMillisTimeSource ^ relNanosTimeSource, String ^ name)
    : MSimpleStopwatch(true)
{
    SKRelNanosAbsMillisTimeSource * pTimeSource = (SKRelNanosAbsMillisTimeSource*) (relNanosTimeSource->getPImpl()->pTimeSource);
    SKSimpleNamedStopwatch * pStopwatch = new SKSimpleNamedStopwatch(
        pTimeSource,
        (char *) System::Runtime::InteropServices::Marshal::StringToHGlobalAnsi(name).ToPointer()
    );
    pImpl = pStopwatch;
}


}

