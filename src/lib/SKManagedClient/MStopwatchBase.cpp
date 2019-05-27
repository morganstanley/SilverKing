#include "StdAfx.h"
#include "MStopwatchBase.h"

#include <string>
using namespace std;
#include <stdint.h>
#include <stdlib.h>
#include "SKStopwatchBase.h"
#include "SKSimpleStopwatch.h"


namespace SKManagedClient {


// control
void MStopwatchBase::start()
{
    ((SKSimpleStopwatch*)pImpl)->start();
}

void MStopwatchBase::stop()
{
    ((SKSimpleStopwatch*)pImpl)->stop();
}

void MStopwatchBase::reset()
{
    ((SKSimpleStopwatch*)pImpl)->reset();
}


// elapsed - The stopwatch Must be STOPPED when calling these methods!
Int64 MStopwatchBase::getElapsedNanos()
{
    Int64 elapsed = ((SKSimpleStopwatch*)pImpl)->getElapsedNanos();
    return elapsed;
}

Int64 MStopwatchBase::getElapsedMillisLong()
{
    Int64 elapsed = ((SKSimpleStopwatch*)pImpl)->getElapsedMillisLong();
    return elapsed;
}

int MStopwatchBase::getElapsedMillis()
{
    return ((SKSimpleStopwatch*)pImpl)->getElapsedMillis();
}

double MStopwatchBase::getElapsedSeconds()
{
    return ((SKSimpleStopwatch*)pImpl)->getElapsedSeconds();
}


// split
Int64 MStopwatchBase::getSplitNanos()
{
    Int64 split = ((SKSimpleStopwatch*)pImpl)->getSplitNanos();
    return split;
}

Int64 MStopwatchBase::getSplitMillisLong()
{
    Int64 split = ((SKSimpleStopwatch*)pImpl)->getSplitMillisLong();
    return split;
}

int MStopwatchBase::getSplitMillis()
{
    return ((SKSimpleStopwatch*)pImpl)->getSplitMillis();
}

double MStopwatchBase::getSplitSeconds()
{
    return ((SKSimpleStopwatch*)pImpl)->getSplitSeconds();
}
        
// misc.
String ^ MStopwatchBase::getName()
{
    std::string strName = ((SKSimpleStopwatch*)pImpl)->getName();
    String ^ name = gcnew String(strName.c_str());
    return name;
}

StopwatchState_M MStopwatchBase::getState()
{
    StopwatchState state = ((SKSimpleStopwatch*)pImpl)->getState();
    return static_cast<StopwatchState_M>(state);
}

String ^ MStopwatchBase::toStringElapsed()
{
    std::string strElapsed = ((SKSimpleStopwatch*)pImpl)->toStringElapsed();
    String ^ elapsed = gcnew String(strElapsed.c_str());
    return elapsed;
}

String ^ MStopwatchBase::toStringSplit()
{
    std::string strSplit = ((SKSimpleStopwatch*)pImpl)->toStringSplit();
    String ^ split = gcnew String(strSplit.c_str());
    return split;
}

String ^ MStopwatchBase::toString()
{
    std::string str = ((SKSimpleStopwatch*)pImpl)->toString();
    String ^ representation = gcnew String(str.c_str());
    return representation;
}


}

