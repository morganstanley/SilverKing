#pragma once

#include "SKManagedClient.h"
#include "MRetrievalOptions.h"

using namespace System;

namespace SKManagedClient {

	ref class MVersionConstraint;
	ref class MOpTimeoutController;
	ref class MSecondaryTarget;

	ref class SKGetOptions_M 
	{
	public:
		void * pGetOptions; // (SKGetOptions*) //
	};

	public ref class MGetOptions : public MRetrievalOptions
	{
	public:
		virtual MRetrievalOptions ^ retrievalType(SKRetrievalType_M retrievalType);
		virtual MRetrievalOptions ^ waitMode(SKWaitMode_M waitMode);
		virtual MRetrievalOptions ^ versionConstraint(MVersionConstraint ^ versionConstraint);
		virtual MRetrievalOptions ^ nonExistenceResponse(SKNonExistenceResponse_M nonExistenceResponse);

		virtual MRetrievalOptions ^ updateSecondariesOnMiss(bool updateSecondariesOnMiss);
		virtual MRetrievalOptions ^ secondaryTargets(HashSet<MSecondaryTarget^> ^ secondaryTargets);
		virtual bool getUpdateSecondariesOnMiss();
		virtual HashSet<MSecondaryTarget^> ^ getSecondaryTargets();
		virtual bool equals(MRetrievalOptions ^ other);

		virtual SKRetrievalType_M getRetrievalType() ;
		virtual SKWaitMode_M getWaitMode() ;
		virtual MVersionConstraint ^ getVersionConstraint() ;
		virtual SKNonExistenceResponse_M getNonExistenceResponse() ;
		virtual bool getVerifyChecksums() ;
		virtual String ^ toString();

		//MGetOptions ^ forwardingMode(SKForwardingMode_M forwardingMode);
		//SKForwardingMode_M getForwardingMode() const;

		//c-tors / d-tors
		virtual ~MGetOptions(void);
		!MGetOptions(void);
		MGetOptions(SKRetrievalType_M retrievalType, MVersionConstraint ^ versionConstraint);
		MGetOptions(SKRetrievalType_M retrievalType);
		MGetOptions(MOpTimeoutController ^ opTimeoutController, SKRetrievalType_M retrievalType, 
			MVersionConstraint ^ versionConstraint, SKNonExistenceResponse_M nonExistenceResponse, 
			bool verifyChecksums,  bool updateSecondariesOnMiss, HashSet<MSecondaryTarget^> ^ secondaryTargets);
		MGetOptions(MOpTimeoutController ^ opTimeoutController, SKRetrievalType_M retrievalType, 
			MVersionConstraint ^ versionConstraint);

	internal:
		MGetOptions(SKGetOptions_M ^ opt);
		SKGetOptions_M ^ getPImpl();

	private:
		void * pImpl;

	};

	//friend bool operator == (MGetOptions ^ go1,  MGetOptions ^ go2) const;
}
