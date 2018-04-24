////#pragma once
#ifndef SKGETOPTIONS_H
#define SKGETOPTIONS_H

#include <set>
#include "skconstants.h"
#include "skbasictypes.h"
#include "SKRetrievalOptions.h"
#include "SKSecondaryTarget.h"

//namespace jace { namespace proxy { namespace com { namespace ms { namespace silverking { namespace cloud { namespace dht (
//	class GetOptions; 
//} } } } } } };
//typedef jace::proxy::com::ms::silverking::cloud::dht::GetOptions GetOptions;
class SKVersionConstraint;
class SKOpTimeoutController;
class SKSecondaryTarget;

class SKGetOptions : public SKRetrievalOptions
{
public:        
	SKAPI SKGetOptions * opTimeoutController(SKOpTimeoutController * opTimeoutController);
	SKAPI SKGetOptions * secondaryTargets(std::set<SKSecondaryTarget*> * secondaryTargets);
	SKAPI SKGetOptions * retrievalType(SKRetrievalType retrievalType);
	SKAPI SKGetOptions * versionConstraint(SKVersionConstraint * versionConstraint);
	SKAPI SKGetOptions * nonExistenceResponse(SKNonExistenceResponse::SKNonExistenceResponse nonExistenceResponse);
	SKAPI SKGetOptions * verifyChecksums(bool verifyChecksums);
	SKAPI SKGetOptions * returnInvalidations(bool returnInvalidations);
	SKAPI SKGetOptions * updateSecondariesOnMiss(bool updateSecondariesOnMiss);
	SKAPI SKGetOptions * forwardingMode(SKForwardingMode forwardingMode);
    
	SKAPI virtual SKForwardingMode getForwardingMode() const;
    
	SKAPI static SKGetOptions * parse(const char * def);
	SKAPI virtual string toString();
	SKAPI virtual bool equals(SKGetOptions * other) const;
    
    SKAPI virtual ~SKGetOptions();
	SKAPI SKGetOptions(SKOpTimeoutController * opTimeoutController, 
        std::set<SKSecondaryTarget*> * secondaryTargets,
        SKRetrievalType retrievalType, SKVersionConstraint * versionConstraint, 
        SKNonExistenceResponse::SKNonExistenceResponse nonExistenceResponse, bool verifyChecksums,  
        bool returnInvalidations, SKForwardingMode forwardingMode, bool updateSecondariesOnMiss);
        
	SKGetOptions(void * pOpt);
	void * getPImpl() const;

protected:
};

#endif // SKGETOPTIONS_H
