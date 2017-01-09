// ReconciliationSet.c

/////////////
// includes

#include "ReconciliationSet.h"
#include "Util.h"

#include <pthread.h>
#include <stdio.h>

#include <set>

////////////////////
// private globals

static pthread_mutex_t			_rcstMutexInstance;
static pthread_mutex_t			*_rcstMutex;
static std::set<std::string>	_reconciliationSet;


///////////////////
// implementation

void rcst_init() {
	_rcstMutex = &_rcstMutexInstance;
    if (pthread_mutex_init(_rcstMutex, NULL) != 0) {
        fatalError("\n mutex init failed", __FILE__, __LINE__);
	}
}

void rcst_add_to_reconciliation_set(char *path) {
    pthread_mutex_lock(_rcstMutex);	
	_reconciliationSet.insert(std::string(path));
    pthread_mutex_unlock(_rcstMutex);	
}

void rcst_remove_from_reconciliation_set(char *path) {
    pthread_mutex_lock(_rcstMutex);	
	_reconciliationSet.erase(std::string(path));
    pthread_mutex_unlock(_rcstMutex);	
}

std::set<std::string> rcst_get_current_set() {
    pthread_mutex_lock(_rcstMutex);	
	std::set<std::string>	copy(_reconciliationSet);
    pthread_mutex_unlock(_rcstMutex);	
	return copy;
}
