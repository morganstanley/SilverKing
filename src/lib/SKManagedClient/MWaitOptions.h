#pragma once
#include "SKManagedClient.h"
#include "MRetrievalOptions.h"

using namespace System;

namespace SKManagedClient {

    ref class MVersionConstraint;
    ref class MWaitForTimeoutController;
    ref class MSecondaryTarget;

    ref class SKWaitOptions_M 
    {
    public: 
        void * pWaitOptions; // (SKWaitOptions*) //
    };

    public ref class MWaitOptions  : public MRetrievalOptions 
    {
    public:
        static MWaitOptions ^ parse(String ^ def);
        MWaitOptions(SKRetrievalType_M retrievalType, MVersionConstraint ^ versionConstraint, int timeoutSeconds, int threshold, SKTimeoutResponse_M timeoutResponse);
        MWaitOptions(SKRetrievalType_M retrievalType, MVersionConstraint ^ versionConstraint, int timeoutSeconds, int threshold);
        MWaitOptions(SKRetrievalType_M retrievalType, MVersionConstraint ^ versionConstraint, int timeoutSeconds);
        MWaitOptions(SKRetrievalType_M retrievalType, MVersionConstraint ^ versionConstraint);
        MWaitOptions(SKRetrievalType_M retrievalType);
        MWaitOptions();
        MWaitOptions(SKRetrievalType_M retrievalType, MVersionConstraint ^ versionConstraint,
                SKNonExistenceResponse_M nonExistenceResponse, bool verifyChecksums,
                bool updateSecondariesOnMiss, int timeoutSeconds, int threshold, SKTimeoutResponse_M timeoutResponse);
        MWaitOptions(SKRetrievalType_M retrievalType, MVersionConstraint ^ versionConstraint,
                SKNonExistenceResponse_M nonExistenceResponse, bool verifyChecksums, 
                bool updateSecondariesOnMiss, HashSet<MSecondaryTarget^> ^ secondaryTargets, int timeoutSeconds, int threshold, 
                SKTimeoutResponse_M timeoutResponse);
        MWaitOptions(MWaitForTimeoutController ^ opTimeoutController, SKRetrievalType_M retrievalType, 
                MVersionConstraint ^ versionConstraint, SKNonExistenceResponse_M nonExistenceResponse, 
                bool verifyChecksums, HashSet<MSecondaryTarget^> ^ secondaryTargets, int timeoutSeconds, int threshold, 
                SKTimeoutResponse_M timeoutResponse, bool updateSecondariesOnMiss );
        MWaitOptions(MWaitForTimeoutController ^ opTimeoutController, SKRetrievalType_M retrievalType, 
                MVersionConstraint ^ versionConstraint, SKNonExistenceResponse_M nonExistenceResponse, 
                bool verifyChecksums, int timeoutSeconds, int threshold, SKTimeoutResponse_M timeoutResponse, 
                bool updateSecondariesOnMiss );
        MWaitOptions(MWaitForTimeoutController ^ opTimeoutController, SKRetrievalType_M retrievalType, 
                MVersionConstraint ^ versionConstraint, int timeoutSeconds, int threshold, 
                SKTimeoutResponse_M timeoutResponse);
        MWaitOptions(MWaitForTimeoutController ^ opTimeoutController, SKRetrievalType_M retrievalType,
                MVersionConstraint ^ versionConstraint, int timeoutSeconds, int threshold);
        virtual MRetrievalOptions ^ updateSecondariesOnMiss(bool updateSecondariesOnMiss);
        virtual MRetrievalOptions ^ secondaryTargets(HashSet<MSecondaryTarget^> ^ secondaryTargets);
        virtual bool getUpdateSecondariesOnMiss();
        virtual HashSet<MSecondaryTarget^> ^ getSecondaryTargets();
        virtual bool equals(MRetrievalOptions ^ other);

        virtual MRetrievalOptions ^ retrievalType(SKRetrievalType_M retrievalType); 
        virtual MRetrievalOptions ^ versionConstraint(MVersionConstraint ^ versionConstraint);
        virtual MRetrievalOptions ^ waitMode(SKWaitMode_M waitMode);
        virtual MRetrievalOptions ^ nonExistenceResponse(SKNonExistenceResponse_M nonExistenceResponse);
        MWaitOptions ^ timeoutSeconds(int timeoutSeconds);
        MWaitOptions ^ threshold(int threshold);
        MWaitOptions ^ timeoutResponse(SKTimeoutResponse_M timeoutResponse);

        virtual SKRetrievalType_M getRetrievalType() ;
        virtual SKWaitMode_M getWaitMode() ;
        virtual MVersionConstraint ^ getVersionConstraint() ;
        virtual SKNonExistenceResponse_M getNonExistenceResponse() ;
        virtual bool getVerifyChecksums() ;

        int getTimeoutSeconds() ;
        int getThreshold() ;
        SKTimeoutResponse_M getTimeoutResponse() ;
        bool hasTimeout() ;
        virtual String ^ toString() ;

        !MWaitOptions();
        virtual ~MWaitOptions();

    internal:
        MWaitOptions(SKWaitOptions_M ^ opt);
        SKWaitOptions_M ^ getPImpl() ;

    private:
        void * pImpl;
    };

    //friend bool operator == (MWaitOptions ^ go1,  MWaitOptions ^ go2) const;

}