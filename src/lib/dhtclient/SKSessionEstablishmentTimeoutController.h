#pragma once
#include "skconstants.h"
#include <limits.h>
#include <string>

class SKSessionOptions;
namespace jace { namespace proxy { namespace com { namespace ms { 
    namespace silverking {namespace cloud { namespace dht { namespace client {
        class SessionEstablishmentTimeoutController;
} } } } } } } }
typedef jace::proxy::com::ms::silverking::cloud::dht::client::SessionEstablishmentTimeoutController SessionEstablishmentTimeoutController;

class SKSessionEstablishmentTimeoutController
{
public:
    /**
     * Return the maximum number of times that this operation should be attempted.
     * @param pSessOpts the relevant SessionOptions 
     * @return the maximum number of times that this operation should be attempted
     */
    SKAPI virtual int getMaxAttempts(SKSessionOptions * pSessOpts)=0;

    /**
     * Return the relative timeout in milliseconds for the given attempt.
     * @param pSessOpts the relevant SessionOptions
     * @param attemptIndex a zero-based attempt index. Ranges from 0 to the maximum number of attempts - 1.
     * @return the relative timeout in milliseconds for the given attempt
     */
    SKAPI virtual int getRelativeTimeoutMillisForAttempt(SKSessionOptions * pSessOpts, int attemptIndex)=0;

    /**
     * Return the maximum relative timeout for the given operation. Once this timeout is triggered, no further
     * attempts of this operation will be made irrespective of the individual attempt timeout or the 
     * maximum number of attempts.
     * @param pSessOpts the relevant SessionOptions
     * @return the maximum relative timeout for the given operation
     */
    SKAPI virtual int getMaxRelativeTimeoutMillis(SKSessionOptions * pSessOpts)=0;

    SKAPI virtual std::string toString()=0;

    SKAPI virtual ~SKSessionEstablishmentTimeoutController();

    virtual SessionEstablishmentTimeoutController * getPImpl()=0;

protected:
    //void * pImpl;

    SKSessionEstablishmentTimeoutController(void);
};

