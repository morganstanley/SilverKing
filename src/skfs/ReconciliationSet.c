// ReconciliationSet.c

/////////////
// includes

#include "ReconciliationSet.h"
#include "Util.h"

#include <pthread.h>
#include <stdio.h>

#include <set>


///////////////////
// public globals

pthread_mutex_t            *rcstMutex;
pthread_cond_t            *rcstCV;


////////////////////
// private globals

static pthread_mutex_t            _rcstMutexInstance;
static pthread_cond_t            _rcstCVInstance;
static std::set<std::string>    _reconciliationSet;


///////////////////
// implementation

void rcst_init() {
    mutex_init(&_rcstMutexInstance, &rcstMutex);
    cv_init(&_rcstCVInstance, &rcstCV);
}

void rcst_add_to_reconciliation_set(char *path) {
    pthread_mutex_lock(rcstMutex);    
    _reconciliationSet.insert(std::string(path));
    pthread_cond_signal(rcstCV);
    pthread_mutex_unlock(rcstMutex);    
}

void rcst_remove_from_reconciliation_set(char *path) {
    pthread_mutex_lock(rcstMutex);    
    _reconciliationSet.erase(std::string(path));
    pthread_mutex_unlock(rcstMutex);    
}

std::set<std::string> rcst_get_current_set() {
    pthread_mutex_lock(rcstMutex);    
    std::set<std::string>    copy(_reconciliationSet);
    pthread_mutex_unlock(rcstMutex);    
    return copy;
}
