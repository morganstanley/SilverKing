#include "SKEncrypterDecrypter.h"

#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using namespace jace;

#include "jace/proxy/com/ms/silverking/cloud/dht/client/crypto/EncrypterDecrypter.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::crypto::EncrypterDecrypter;


////////


string * SKEncrypterDecrypter::getName() const {
	//string representation = (string)(((EncrypterDecrypter*)pImpl)->toString());
	//return representation;
    return NULL;
}

