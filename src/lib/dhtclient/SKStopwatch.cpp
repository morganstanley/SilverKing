#include "SKStopwatch.h"

#include "jace/proxy/java/lang/String.h"
using jace::proxy::java::lang::String;
#include "jace/proxy/com/ms/silverking/time/Stopwatch.h"
using jace::proxy::com::ms::silverking::time::Stopwatch;
#include "jace/proxy/com/ms/silverking/time/Stopwatch_State.h"
using jace::proxy::com::ms::silverking::time::Stopwatch_State;

//impl
SKStopwatch::SKStopwatch(Stopwatch * pStopwatch) {
	if(pStopwatch)
		pImpl = pStopwatch;
} 
	
SKStopwatch::~SKStopwatch()
{
	if(pImpl!=NULL) {
		delete pImpl; 
		pImpl = NULL;
	}
}

Stopwatch * SKStopwatch::getPImpl(){
	return pImpl ;
}

// control
void SKStopwatch::start(){
	getPImpl()->start();
}
void SKStopwatch::stop(){
	getPImpl()->stop();
}

void SKStopwatch::reset(){
	getPImpl()->reset();
}

// elapsed 
int64_t SKStopwatch::getElapsedNanos(){
	return (int64_t)(getPImpl()->getElapsedNanos());
}

int64_t SKStopwatch::getElapsedMillisLong(){
	return (int64_t)(getPImpl()->getElapsedMillisLong());
}

int SKStopwatch::getElapsedMillis(){
	return (int)(getPImpl()->getElapsedMillis());
}

double SKStopwatch::getElapsedSeconds(){
	return (double)(getPImpl()->getElapsedSeconds());
}

// split
int64_t SKStopwatch::getSplitNanos(){
	return (int64_t)((Stopwatch*)getPImpl())->getSplitNanos();
}

int64_t SKStopwatch::getSplitMillisLong(){
	return (int64_t)(getPImpl()->getSplitMillisLong());
}
int SKStopwatch::getSplitMillis(){
	return (int)(getPImpl()->getSplitMillis());
}

double SKStopwatch::getSplitSeconds(){
	return (double)(getPImpl()->getSplitSeconds());
}

// misc.
string SKStopwatch::getName(){
	return (string)(getPImpl()->getName());
}

StopwatchState SKStopwatch::getState(){
	int currState = (int)(getPImpl()->getState().ordinal());
	return static_cast<StopwatchState>(currState);
}

string SKStopwatch::toStringElapsed(){
	return (string)(getPImpl()->toStringElapsed());
}

string SKStopwatch::toStringSplit(){
	return (string)(getPImpl()->toStringSplit());
}


string SKStopwatch::toString(){
	return (string)(getPImpl()->toString());
}
