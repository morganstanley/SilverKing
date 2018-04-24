#ifndef SKRETRIEVALOPTIONS_H
#define SKRETRIEVALOPTIONS_H

#include <string>
using std::string;
#include <set>

#include "skconstants.h"
#include "skbasictypes.h"
#include "SKOperationOptions.h"
#include "SKOpTimeoutController.h"
#include "SKSecondaryTarget.h"

//namespace jace { namespace proxy { namespace com { namespace ms { 
//    namespace silverking { namespace cloud { namespace dht { 
//	    class RetrievalOptions;
//} } } } } } };
//typedef jace::proxy::com::ms::silverking::cloud::dht::RetrievalOptions RetrievalOptions;

class SKVersionConstraint;
class SKSecondaryTarget;

class SKRetrievalOptions : public SKOperationOptions
{
public:
	SKAPI SKRetrievalOptions * opTimeoutController(SKOpTimeoutController *opTimeoutController);
	SKAPI SKRetrievalOptions * secondaryTargets(std::set<SKSecondaryTarget*> * secondaryTargets);
	SKAPI SKRetrievalOptions * retrievalType(SKRetrievalType retrievalType);
	SKAPI SKRetrievalOptions * waitMode(SKWaitMode waitMode);
	SKAPI SKRetrievalOptions * versionConstraint(SKVersionConstraint * versionConstraint);
	SKAPI SKRetrievalOptions * nonExistenceResponse(SKNonExistenceResponse::SKNonExistenceResponse nonExistenceResponse);
	SKAPI SKRetrievalOptions * verifyChecksums(bool verifyChecksums);
	SKAPI SKRetrievalOptions * returnInvalidations(bool returnInvalidations);
	SKAPI virtual SKRetrievalOptions * forwardingMode(SKForwardingMode forwardingMode);
	SKAPI SKRetrievalOptions * updateSecondariesOnMiss(bool updateSecondariesOnMiss);
    
	SKAPI SKRetrievalType getRetrievalType() const;
	SKAPI SKWaitMode getWaitMode() const;
	SKAPI SKVersionConstraint * getVersionConstraint() const;
	SKAPI SKNonExistenceResponse::SKNonExistenceResponse getNonExistenceResponse() const;
	SKAPI bool getVerifyChecksums() const;
	SKAPI bool getReturnInvalidations() const;
	SKAPI virtual SKForwardingMode getForwardingMode() const;
    SKAPI bool getUpdateSecondariesOnMiss() const;
    
	SKAPI static SKRetrievalOptions * parse(const char * def);
	SKAPI virtual bool equals(SKRetrievalOptions * other) const;
	SKAPI virtual string toString() const;
	
	//c-tors / d-tors
    SKAPI virtual ~SKRetrievalOptions();

	SKAPI SKRetrievalOptions(SKOpTimeoutController * opTimeoutController, 
            std::set<SKSecondaryTarget*> * secondaryTargets,
            SKRetrievalType retrievalType, 
            SKWaitMode waitMode, SKVersionConstraint * versionConstraint,
            SKNonExistenceResponse::SKNonExistenceResponse nonExistenceResponse, 
			bool verifyChecksums, bool returnInvalidations,
            SKForwardingMode forwardingMode, 
			bool updateSecondariesOnMiss);

protected:	
	void * pImpl;

	SKRetrievalOptions(); 
	SKRetrievalOptions(void * pRetrievalOptImpl);  //FIXME: make protected / move to children ?
};

#endif // SKRETRIEVALOPTIONS_H
