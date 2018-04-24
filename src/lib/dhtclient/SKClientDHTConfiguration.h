#ifndef SKCLIENTDHTCONFIGURATION_H
#define SKCLIENTDHTCONFIGURATION_H

#include <string>
#include <map>
using std::string;
using std::map;

#include "skconstants.h"
#include "SKClientDHTConfigurationProvider.h"

class SKAddrAndPort;

class SKClientDHTConfiguration : public SKClientDHTConfigurationProvider 
{
public:
	SKAPI static SKClientDHTConfiguration * create(map<string,string> * envMap);

	//SKAPI SKClientDHTConfiguration(const char * dhtName, int dhtPort, SKAddrAndPort zkLocs[]);
    //SKAPI SKClientDHTConfiguration(const char * dhtName, SKAddrAndPort zkLocs[]);
    SKAPI SKClientDHTConfiguration(const char * dhtName, const char * zkLocs);
    SKAPI SKClientDHTConfiguration(const char * dhtName, int dhtPort, const char * zkLocs);

	SKAPI char * getName();
    SKAPI int getPort();
	//SKAPI SKAddrAndPort* getZkLocs();   //should be deallocated with delete[]
	SKAPI char * toString();
    SKAPI bool hasPort();
    SKAPI virtual SKClientDHTConfiguration * getClientDHTConfiguration();
	SKAPI virtual ~SKClientDHTConfiguration();

	SKClientDHTConfiguration(void * pClientDHTConfiguration);
protected:
};

#endif // SKCLIENTDHTCONFIGURATION_H
