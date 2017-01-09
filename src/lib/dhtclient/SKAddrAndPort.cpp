#include "SKAddrAndPort.h"
#include "jace/proxy/com/ms/silverking/net/AddrAndPort.h"
using jace::proxy::com::ms::silverking::net::AddrAndPort;

class AddrAndPortImpl{
public:

	AddrAndPort addrAndPort;
};


SKAddrAndPort::SKAddrAndPort(AddrAndPort * pAddrAndPort) { //FIXME: ?
	if(pAddrAndPort)
		pImpl = pAddrAndPort;
}

SKAddrAndPort::~SKAddrAndPort() {
	if(pImpl) {
		delete pImpl;
		pImpl = NULL;
	}
};

AddrAndPort * SKAddrAndPort::getPImpl(){
	return pImpl;
}

/*
SKnetSocketAddress SKAddrAndPort::toInetSocketAddress(){
	SKnetSocketAddress inetSocketAddress = java_cast<SKnetSocketAddress>(((AddrAndPort*)pImpl)->toInetSocketAddress()) ;
	return inetSocketAddress;
}
*/
