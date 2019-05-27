// ReaderStats.c

/////////////
// includes

#include "ReaderStats.h"
#include "Util.h"


///////////////////
// implementation

ReaderStats *rs_new() {
    ReaderStats *rs;

    rs = (ReaderStats *)mem_alloc(1, sizeof(ReaderStats));
    rs->cache = 0;
    rs->opWait = 0;
    rs->dht = 0;
    rs->nfs = 0;
    pthread_spin_init(&rs->lock, 0);
    return rs;
}

void rs_delete(ReaderStats **rs) {
    if (rs != NULL && *rs != NULL) {
        pthread_spin_destroy(&(*rs)->lock);
        mem_free((void **)rs);
    } else {
        fatalError("bad ptr in rs_delete");
    }
}

void rs_cache_inc(ReaderStats *rs) {
    pthread_spin_lock(&rs->lock);
    rs->cache++;
    pthread_spin_unlock(&rs->lock);
}

void rs_opWait_inc(ReaderStats *rs) {
    pthread_spin_lock(&rs->lock);
    rs->opWait++;
    pthread_spin_unlock(&rs->lock);
}

void rs_dht_inc(ReaderStats *rs) {
    pthread_spin_lock(&rs->lock);
    rs->dht++;
    pthread_spin_unlock(&rs->lock);
}

void rs_nfs_inc(ReaderStats *rs) {
    pthread_spin_lock(&rs->lock);
    rs->nfs++;
    pthread_spin_unlock(&rs->lock);
}

void rs_display(ReaderStats *rs) {
    pthread_spin_lock(&rs->lock);
    srfsLog(LOG_WARNING, "cache: \t%lu", rs->cache);
    srfsLog(LOG_WARNING, "opWait:\t%lu", rs->opWait);
    srfsLog(LOG_WARNING, "dht:   \t%lu", rs->dht);
    srfsLog(LOG_WARNING, "nfs:   \t%lu", rs->nfs);
    pthread_spin_unlock(&rs->lock);
}
