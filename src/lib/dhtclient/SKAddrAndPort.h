#ifndef SKADDRANDPORT_H
#define SKADDRANDPORT_H

#include <cstddef>

//class AddrAndPortImpl;

namespace jace { namespace proxy { namespace com { namespace ms { 
	namespace silverking {namespace net {
		class AddrAndPort;
} } } } } }
typedef jace::proxy::com::ms::silverking::net::AddrAndPort AddrAndPort;

class SKAddrAndPort
{
public:

    ~SKAddrAndPort();
    SKAddrAndPort(AddrAndPort * pAddrAndPort = NULL);
	AddrAndPort * getPImpl();  //FIXME
	
	//SKnetSocketAddress toInetSocketAddress();  //FIXME:
	
protected:
	AddrAndPort * pImpl;
};

#endif // SKADDRANDPORT_H
