#ifndef SKCONSTANTABSMILLISTIMESOURCE_H
#define SKCONSTANTABSMILLISTIMESOURCE_H

#include "skconstants.h"
#include "SKAbsMillisTimeSource.h"

namespace jace { namespace proxy { namespace com { namespace ms { 
    namespace silverking {namespace time { 
        class ConstantAbsMillisTimeSource;
} } } } } }
typedef jace::proxy::com::ms::silverking::time::ConstantAbsMillisTimeSource ConstantAbsMillisTimeSource;

class SKConstantAbsMillisTimeSource : public SKAbsMillisTimeSource
{
public:
    SKAPI SKConstantAbsMillisTimeSource(int64_t absMillisTime);
    SKAPI virtual ~SKConstantAbsMillisTimeSource();

    SKConstantAbsMillisTimeSource(ConstantAbsMillisTimeSource * pConstantAbsMillisTimeSource);
};

#endif //SKCONSTANTABSMILLISTIMESOURCE_H
