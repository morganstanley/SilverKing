#ifndef SKOPERATIONOPTIONS_H
#define SKOPERATIONOPTIONS_H

#include <string>
using std::string;
#include <set>

#include "skconstants.h"
#include "skbasictypes.h"

//namespace jace { namespace proxy { namespace com { namespace ms { 
//    namespace silverking { namespace cloud { namespace dht { 
//	    class OperationOptions;
//} } } } } } };
//typedef jace::proxy::com::ms::silverking::cloud::dht::OperationOptions OperationOptions;

class SKOpTimeoutController;
class SKSecondaryTarget;

class SKOperationOptions
{
public:
	SKAPI SKOpTimeoutController * getOpTimeoutController();
	SKAPI std::set<SKSecondaryTarget*> * getSecondaryTargets();
    
	SKAPI virtual bool equals(SKOperationOptions * other) const;
	SKAPI virtual string toString() const;
	
	//c-tors / d-tors
    SKAPI virtual ~SKOperationOptions();

	SKAPI SKOperationOptions(SKOpTimeoutController * opTimeoutController, 
            std::set<SKSecondaryTarget*> * secondaryTargets);

protected:	
	void * pImpl;

	SKOperationOptions(); 
	SKOperationOptions(void * pOperationOptImpl);  //FIXME: make protected / move to children ?
};

#endif // SKOPERATIONOPTIONS_H
