// HashTableAndLock.h

#ifndef _HASH_TABLE_AND_LOCK_H_
#define _HASH_TABLE_AND_LOCK_H_

/////////////
// includes

#include "hashtable.h"
#include "hashtable_itr.h"

#include <pthread.h>


//////////
// types

typedef struct HashTableAndLock {
    hashtable           *ht;
    pthread_rwlock_t    rwLock;
} HashTableAndLock;

#endif

