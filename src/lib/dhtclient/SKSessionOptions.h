#ifndef SKSESSIONOPTIONS_H
#define SKSESSIONOPTIONS_H

#include "skconstants.h"

//class SessionOptions;

class SKClientDHTConfiguration;
class SKSessionEstablishmentTimeoutController;

class SKSessionOptions
{
public:
    //methods
    SKAPI SKClientDHTConfiguration * getDHTConfig();
    SKAPI char * getPreferredServer();  //caller should free
    SKAPI char * toString();  //caller should free
    SKAPI SKSessionOptions * preferredServer(const char * preferredServer);

    SKAPI void setDefaultTimeoutController(SKSessionEstablishmentTimeoutController * pDefaultTimeoutController);
    SKAPI static SKSessionEstablishmentTimeoutController * getDefaultTimeoutController();
    SKAPI SKSessionEstablishmentTimeoutController * getTimeoutController();
    SKAPI SKSessionOptions(SKClientDHTConfiguration * dhtConfig, const char * preferredServer,
               SKSessionEstablishmentTimeoutController * pTimeoutController);

    SKAPI SKSessionOptions(SKClientDHTConfiguration * dhtConfig, const char * preferredServer);
    SKAPI SKSessionOptions(SKClientDHTConfiguration * dhtConfig);
    SKAPI virtual ~SKSessionOptions();

    SKSessionOptions(void * pSesOptions); //SessionOptions *
    void * getPImpl();
private:
    void * pImpl;

};

#endif // SKSESSIONOPTIONS_H
