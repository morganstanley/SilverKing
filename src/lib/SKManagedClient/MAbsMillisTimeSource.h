#pragma once

using namespace System;

namespace SKManagedClient {

    ref class SKAbsMillisTimeSource_M 
    {
    internal:
        void * pTimeSource;
    };

    public ref class MAbsMillisTimeSource
    {
    public:
        /**
        * @return the difference, measured in milliseconds, between the current time and midnight, January 1, 1970 UTC.
        */
        virtual Int64 absTimeMillis();

        /**
        * @return the difference between absDeadlineMillis and absTimeMillis() 
        */
        virtual int relMillisRemaining(Int64 absDeadlineMillis);

        !MAbsMillisTimeSource();
        ~MAbsMillisTimeSource();
    internal:
        SKAbsMillisTimeSource_M ^ getPImpl();
        MAbsMillisTimeSource(SKAbsMillisTimeSource_M ^ timeSrc);

    protected:
        void * pImpl;

        MAbsMillisTimeSource();
    };

}

