#pragma once

#include "SKManagedClient.h"

using namespace System;
using namespace System::Collections;
using namespace System::Collections::Generic;

namespace SKManagedClient {

    ref class MVersionConstraint;
    ref class MSecondaryTarget;

    public interface class MRetrievalOptions
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
        virtual String ^ toString() ;

        //virtual MGetOptions ^ forwardingMode(SKForwardingMode_M forwardingMode) ;
        //virtual SKForwardingMode_M getForwardingMode() ;

    };

    //friend bool operator == (MGetOptions ^ go1,  MGetOptions ^ go2) const;
}
