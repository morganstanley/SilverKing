#ifndef SKCLIENTDHTCONFIGURATIONPROVIDER_H
#define SKCLIENTDHTCONFIGURATIONPROVIDER_H

#include <cstddef>

class SKClientDHTConfiguration;

class SKClientDHTConfigurationProvider 
{
public:

    virtual SKClientDHTConfiguration * getClientDHTConfiguration();

	//impl
    virtual ~SKClientDHTConfigurationProvider();
    SKClientDHTConfigurationProvider(void * pClientDHTConfigurationProvider = NULL);
	void * getPImpl();  //FIXME
	
protected:
	void * pImpl;
};

#endif // SKCLIENTDHTCONFIGURATIONPROVIDER_H
