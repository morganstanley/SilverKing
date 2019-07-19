#include <string>
using std::string;
#include "jenumutil.h"
#include "SKVersionConstraint.h"

#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using jace::instanceof;
using namespace jace;
#include "jace/proxy/types/JLong.h"
using jace::proxy::types::JLong;
#include "jace/proxy/com/ms/silverking/cloud/dht/VersionConstraint.h"
using jace::proxy::com::ms::silverking::cloud::dht::VersionConstraint;
#include "jace/proxy/com/ms/silverking/cloud/dht/VersionConstraint_Mode.h"
using jace::proxy::com::ms::silverking::cloud::dht::VersionConstraint_Mode;

// -------- static functions -------- //

SKVersionConstraint * SKVersionConstraint::exactMatch(int64_t version){
    VersionConstraint * pvc = new VersionConstraint(java_cast<VersionConstraint>(VersionConstraint::exactMatch(version))); 
    return new SKVersionConstraint(pvc);
}

SKVersionConstraint * SKVersionConstraint::maxAboveOrEqual(int64_t threshold){
    VersionConstraint * pvc = new VersionConstraint(java_cast<VersionConstraint>(VersionConstraint::maxAboveOrEqual(threshold))); 
    return new SKVersionConstraint(pvc);
}

SKVersionConstraint * SKVersionConstraint::maxBelowOrEqual(int64_t threshold){
    VersionConstraint * pvc = new VersionConstraint(java_cast<VersionConstraint>(VersionConstraint::maxBelowOrEqual(threshold))); 
    return new SKVersionConstraint(pvc);
}

SKVersionConstraint * SKVersionConstraint::minAboveOrEqual(int64_t threshold){
    VersionConstraint * pvc = new VersionConstraint(java_cast<VersionConstraint>(VersionConstraint::minAboveOrEqual(threshold))); 
    return new SKVersionConstraint(pvc);
}

// -------- c-tors / d-tors -------- //

SKVersionConstraint::SKVersionConstraint(int64_t minVersion, int64_t maxVersion, SKVersionConstraintMode mode, int64_t maxStorageTime){
    VersionConstraint_Mode * pVCM = getVersionConstraintMode(mode);
    pImpl = new VersionConstraint(java_new<VersionConstraint>(minVersion, maxVersion, *pVCM, maxStorageTime)); 
    delete pVCM;
}

SKVersionConstraint::SKVersionConstraint(int64_t minVersion, int64_t maxVersion, SKVersionConstraintMode mode){
    VersionConstraint_Mode * pVCM = getVersionConstraintMode(mode);
    pImpl = new VersionConstraint(java_new<VersionConstraint>(minVersion, maxVersion, *pVCM)); 
    delete pVCM;
}

SKVersionConstraint::~SKVersionConstraint()
{
    if(pImpl!=NULL) {
        delete pImpl; 
        pImpl = NULL;
    }
}

//private ctor
SKVersionConstraint::SKVersionConstraint(VersionConstraint * pOpt) : pImpl(pOpt) {};

VersionConstraint * SKVersionConstraint::getPImpl() {return pImpl;}


// -------- public methods -------- //

int64_t SKVersionConstraint::getMax(){
    return static_cast<int64_t>(pImpl->getMax());
}

int64_t SKVersionConstraint::getMaxCreationTime(){
    VersionConstraint * pVc = (VersionConstraint*)pImpl;
    return static_cast<int64_t>(pVc->getMaxCreationTime());
}

int64_t SKVersionConstraint::getMin(){
    VersionConstraint * pVc = (VersionConstraint*)pImpl;
    return static_cast<int64_t>(pVc->getMin());
}

SKVersionConstraintMode SKVersionConstraint::getMode(){
    int mode = (int)(pImpl->getMode().ordinal());
    return static_cast<SKVersionConstraintMode>(mode);
}

bool SKVersionConstraint::matches(int64_t version){
    jboolean matched = (jboolean)(pImpl->matches(version));
    return static_cast<bool>(matched);
}

bool SKVersionConstraint::overlaps(SKVersionConstraint * other){
    VersionConstraint * pVc = pImpl;
    VersionConstraint * pVc2 = other->pImpl;
    jboolean matched = (jboolean)(pVc->overlaps( *pVc2 ));
    return static_cast<bool>(matched);
}

bool SKVersionConstraint::equals(SKVersionConstraint * other){
    VersionConstraint * pVc = pImpl;
    VersionConstraint * pVc2 = other->pImpl;
    jboolean matched = (jboolean)(pVc->equals( *pVc2 ));
    return static_cast<bool>(matched);
}

string SKVersionConstraint::toString(){
    string representation = (string)(pImpl->toString());
    return representation;
}


SKVersionConstraint * SKVersionConstraint::max(int64_t newMaxVal){
    VersionConstraint * pVersionConstrImp = new VersionConstraint(java_cast<VersionConstraint>(
        pImpl->max(JLong(newMaxVal))
    )); 
    delete (pImpl);
    pImpl = pVersionConstrImp;
    return this;
}

SKVersionConstraint * SKVersionConstraint::min(int64_t newMinVal){
    VersionConstraint * pVersionConstrImp = new VersionConstraint(java_cast<VersionConstraint>(
        (pImpl)->min(JLong(newMinVal))
    )); 
    delete pImpl;
    pImpl = pVersionConstrImp;
    return this;
}

SKVersionConstraint * SKVersionConstraint::maxCreationTime(int64_t maxStorageTime){
    VersionConstraint * pVersionConstrImp = new VersionConstraint(java_cast<VersionConstraint>(
        pImpl->maxCreationTime(JLong(maxStorageTime))
    )); 
    delete pImpl;
    pImpl = pVersionConstrImp;
    return this;
}

SKVersionConstraint * SKVersionConstraint::mode(SKVersionConstraintMode mode){
    VersionConstraint_Mode * pVCM = getVersionConstraintMode(mode);
    VersionConstraint * pVersionConstrImp = new VersionConstraint(java_cast<VersionConstraint>(
        pImpl->mode(*pVCM)
    )); 
    delete pImpl;
    delete pVCM;
    pImpl = pVersionConstrImp;
    return this;
}


