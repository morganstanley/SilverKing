#ifndef SKSIMPLENAMEDSTOPWATCH_H
#define SKSIMPLENAMEDSTOPWATCH_H

#include <string>
using std::string;
#include "SKSimpleStopwatch.h"

class SKRelNanosTimeSource;
class SKRelNanosAbsMillisTimeSource;
namespace jace { namespace proxy { namespace com { namespace ms { 
    namespace silverking { namespace time {
	    class SimpleNamedStopwatch;
} } } } } };
typedef jace::proxy::com::ms::silverking::time::SimpleNamedStopwatch SimpleNamedStopwatch;

class SKSimpleNamedStopwatch : public SKSimpleStopwatch
{
public:
	SKAPI string getName();

	SKAPI SKSimpleNamedStopwatch();
	SKAPI SKSimpleNamedStopwatch(const char * name);
	SKAPI SKSimpleNamedStopwatch(SKRelNanosAbsMillisTimeSource * relNanosTimeSource, const char * name);
    SKAPI virtual ~SKSimpleNamedStopwatch();
protected:	
	SKSimpleNamedStopwatch(SimpleNamedStopwatch * pSimpleNamedStopwatch);
};

#endif // SKSIMPLENAMEDSTOPWATCH_H
