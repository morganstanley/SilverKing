#ifndef SKCLIENT_H
#define SKCLIENT_H

#include <cstddef>
#include "skconstants.h"

class SKSession;
class SKClientDHTConfigurationProvider;
class SKSessionOptions;
class SKValueCreator;
class SKGridConfiguration;

namespace jace { namespace proxy { namespace com { namespace ms { 
    namespace silverking {namespace cloud { namespace dht { namespace client {
        class DHTClient;
} } } } } } } }
typedef jace::proxy::com::ms::silverking::cloud::dht::client::DHTClient DHTClient;


class SKClient
{
public:
  SKAPI static bool init(LoggingLevel level, const char * pJvmOptions = NULL) ; 
  SKAPI static void shutdown();
  SKAPI static SKClient *getClient();
  SKAPI static SKClient *getClient(LoggingLevel level);
  SKAPI static SKClient *getClient(LoggingLevel level, const char *pJvmOptions);
  SKAPI static void setLogLevel(LoggingLevel level);
  SKAPI static void setLogFile(const char * fileName);
  /* to be called from a new thread; 
    returns: true on success, false if client is not inited; 
    throws: JNIException, VirtualMachineShutdownError (std::excpetion)
  */
  SKAPI static bool attach(bool daemon = false) ; 
  /* to be called  from a thread before it shuts down */
  SKAPI static void detach() ; 

  SKAPI static SKValueCreator * getValueCreator();
  SKAPI virtual ~SKClient();
  SKAPI SKSession * openSession(SKGridConfiguration * pGridConf);
  SKAPI SKSession * openSession(SKClientDHTConfigurationProvider * dhtConfigProvider);
  SKAPI SKSession * openSession(SKGridConfiguration * pGridConf, const char * preferredServer);
  SKAPI SKSession * openSession(SKSessionOptions * sessionOptions);
  
private:
  static SKClient * pClient;    
  DHTClient * pImpl;

  SKClient();
};

#endif  //SKCLIENT_H

