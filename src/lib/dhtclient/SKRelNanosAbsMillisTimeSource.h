#ifndef SKRELNANOSABSMILLISTIMESOURCE_H
#define SKRELNANOSABSMILLISTIMESOURCE_H

#include "skconstants.h"
#include "SKAbsMillisTimeSource.h"

namespace jace { namespace proxy { namespace com { namespace ms { 
	namespace silverking {namespace time {
		class RelNanosAbsMillisTimeSource;
} } } } } }
typedef jace::proxy::com::ms::silverking::time::RelNanosAbsMillisTimeSource RelNanosAbsMillisTimeSource;
 

class SKRelNanosAbsMillisTimeSource : public SKAbsMillisTimeSource /*, public SKRelNanosTimeSource */
{
public:
    SKRelNanosAbsMillisTimeSource(RelNanosAbsMillisTimeSource * pRelNanosAbsMillisTimeSource);
    SKAPI virtual ~SKRelNanosAbsMillisTimeSource();
	SKAPI virtual int64_t relTimeNanos();

protected:
    SKRelNanosAbsMillisTimeSource();
    SKRelNanosAbsMillisTimeSource(const SKRelNanosAbsMillisTimeSource&);
    const SKRelNanosAbsMillisTimeSource & operator =(const SKRelNanosAbsMillisTimeSource &);

};

#endif //SKRELNANOSABSMILLISTIMESOURCE_H
