#ifndef SKSECONDARYTARGET_H
#define SKSECONDARYTARGET_H

#include "skconstants.h"
#include <string>
using std::string;

namespace jace { namespace proxy { namespace com { namespace ms { 
    namespace silverking { namespace cloud { namespace dht {
	    class SecondaryTarget;
} } } } } } };
typedef jace::proxy::com::ms::silverking::cloud::dht::SecondaryTarget SecondaryTarget;

class SKSecondaryTarget
{
public:
	//methods
    SKAPI SKSecondaryTargetType getType();
    SKAPI char * getTarget(); //caller should free
	SKAPI char * toString();  //caller should free

	SKAPI SKSecondaryTarget(SKSecondaryTargetType type, const char * target);
	SKAPI SKSecondaryTarget(SKSecondaryTargetType type, std::string target);
    SKAPI virtual ~SKSecondaryTarget();

	SKSecondaryTarget(SecondaryTarget * pSecondaryTarget); //SecondaryTarget *
    SecondaryTarget * getPImpl();
private:
	SecondaryTarget * pImpl;

};

#endif // SKSECONDARYTARGET_H

