// ReaderStats.h

#ifndef _READER_STATS_H_
#define _READER_STATS_H_

/////////////
// includes

#include <pthread.h>
#include <stdint.h>


//////////
// types

typedef struct ReaderStats {
	uint64_t	cache;
	uint64_t	opWait;
	uint64_t	dht;
	uint64_t	nfs;
	pthread_spinlock_t	lock;
} ReaderStats;


///////////////
// prototypes

ReaderStats *rs_new();
void rs_delete(ReaderStats **rs);
void rs_cache_inc(ReaderStats *rs);
void rs_opWait_inc(ReaderStats *rs);
void rs_dht_inc(ReaderStats *rs);
void rs_nfs_inc(ReaderStats *rs);
void rs_display(ReaderStats *rs);

#endif
