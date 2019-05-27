#pragma once
#include "SKManagedClient.h"

using namespace System;

namespace SKManagedClient {

    ref class SKVersionConstraint_M 
    {
    public:
        void * pVersionConstraint; // (SKVersionConstraint*) //
    };


    public ref class MVersionConstraint
    {
    public:
        static MVersionConstraint ^ exactMatch(Int64 version);
        static MVersionConstraint ^ maxAboveOrEqual(Int64 threshold);
        static MVersionConstraint ^ maxBelowOrEqual(Int64 threshold);
        static MVersionConstraint ^ minAboveOrEqual(Int64 threshold);

        MVersionConstraint(Int64 minVersion, Int64 maxVersion, SKVersionConstraintMode_M mode, Int64 maxStorageTime);
        MVersionConstraint(Int64 minVersion, Int64 maxVersion, SKVersionConstraintMode_M mode);
        ~MVersionConstraint();
        !MVersionConstraint();

        Int64 getMax();
        Int64 getMaxCreationTime();
        Int64 getMin();
        SKVersionConstraintMode_M getMode();
        bool matches(Int64 version);
        bool overlaps(MVersionConstraint ^ other);
        bool equals(MVersionConstraint ^ other);
        String ^ toString();

        MVersionConstraint ^ max(Int64 newMaxVal);
        MVersionConstraint ^ min(Int64 newMinVal);
        MVersionConstraint ^ mode(SKVersionConstraintMode_M mode);
        MVersionConstraint ^ maxCreationTime(Int64 maxCreationTime);

    internal:
        MVersionConstraint(SKVersionConstraint_M ^ verConstraint);
        SKVersionConstraint_M ^ getPImpl();

    private:
        void * pImpl;
    };


}