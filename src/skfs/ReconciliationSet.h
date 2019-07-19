// OpenDir.h

#ifndef _RECONCILIATION_SET_H_
#define _RECONCILIATION_SET_H_

/////////////
// includes

#include <pthread.h>

#include <set>
#include <string>


///////////////////
// public globals

extern pthread_mutex_t            *rcstMutex;
extern pthread_cond_t            *rcstCV;


//////////////////////
// public prototypes

void rcst_init();
void rcst_add_to_reconciliation_set(char *path);
void rcst_remove_from_reconciliation_set(char *path);
std::set<std::string> rcst_get_current_set();

#endif /* _RECONCILIATION_SET_H_ */
