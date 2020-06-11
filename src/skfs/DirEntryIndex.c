// DirEntryIndex.c

/////////////
// includes

#include "DirEntryIndex.h"
#include "Util.h"


////////////
// defines

//#define DE_DEBUG

///////////////////////
// private prototypes

//static void dei_reindex(DirEntryIndex *dei, DirData *dd);


////////////////////////////////
// DEIndexEntry implementation

DEIndexEntry *deie_set(DEIndexEntry *indexEntry, DirData *dd, DirEntry *de) {
    void    *base;
    
    base = (void *)dd->data;
    *indexEntry = (uint32_t)ptr_to_offset(base, (void *)de);
    srfsLog(LOG_FINE, "deie_set %llx %d %llx", indexEntry, *indexEntry, (indexEntry + 1));
    return indexEntry + 1;
}


/////////////////////////////////
// DirEntryIndex implementation

/**
 * Initialize the DirEntryIndex header.
 */
void dei_init(DirEntryIndex *dei, uint32_t numEntries) {
    dei->magic = DEI_MAGIC;
    dei->numEntries = numEntries;
}

int dei_sanity_check(DirEntryIndex *dei, int fatalErrorOnFailure) {
    if (dei->magic != DEI_MAGIC) {
        srfsLog(LOG_ERROR, "dei->magic != DEI_MAGIC  dei %llx dei->magic %x != %x", dei, dei->magic, DEI_MAGIC);
        if (fatalErrorOnFailure) {
            fatalError("dei->magic != DEI_MAGIC", __FILE__, __LINE__);
        }
        return FALSE;
    } else {
        return TRUE;
    }
}

/**
 * Given an initialized DirEntryIndex (correct numEntries), and 
 * the first DirEntry in a DirEntry chain, create an unsorted
 * index. dei_reindex should be called after this function.
 */
void dei_create_unsorted_index(DirEntryIndex *dei, DirEntry *de, DirEntry *limit) {
    uint32_t    i;
    uint32_t    numEntries;
    DirEntry    *base;
    
    base = de;
    i = 0;
    while (de != NULL) {
        dei->entries[i] = (uint32_t)((uint64_t)de - (uint64_t)base);
        i++;
        de = de_next(de, limit, TRUE);
    }
    numEntries = i;
    if (numEntries != dei->numEntries) {
        fatalError("numEntries != dei->numEntries", __FILE__, __LINE__);
    }
}

__thread void *thread_context;

/**
 * Given a DirData as context, and two DEIndexEntries (offsets), 
 * compare the two DirEntries that at the offsets using the names of 
 * the entries.
 */
static int dei_comp_old(const void *o1, const void *o2) {
    DEIndexEntry    *e1;
    DEIndexEntry    *e2;
    DirData    *dd;
    DirEntry    *de1;
    DirEntry    *de2;
    void *context;
    
    context = thread_context;
    e1 = (DEIndexEntry *)o1;
    e2 = (DEIndexEntry *)o2;
    dd = (DirData *)context;
    de1 = (DirEntry *)offset_to_ptr(dd->data, *e1);
    de2 = (DirEntry *)offset_to_ptr(dd->data, *e2);
    de_sanity_check(de1);
    de_sanity_check(de2);
    return strncmp(de_get_name(de1), de_get_name(de2), SRFS_MAX_PATH_LENGTH);
}

/**
 * Given a DirData as context, and two DEIndexEntries (offsets), 
 * compare the two DirEntries that at the offsets using the names of 
 * the entries.
 */
static int dei_comp(const void *o1, const void *o2, void *context) {
    DEIndexEntry    *e1;
    DEIndexEntry    *e2;
    DirData    *dd;
    DirEntry    *de1;
    DirEntry    *de2;
    
    e1 = (DEIndexEntry *)o1;
    e2 = (DEIndexEntry *)o2;
    dd = (DirData *)context;
    de1 = (DirEntry *)offset_to_ptr(dd->data, *e1);
    de2 = (DirEntry *)offset_to_ptr(dd->data, *e2);
    de_sanity_check(de1);
    de_sanity_check(de2);
    return strncmp(de_get_name(de1), de_get_name(de2), SRFS_MAX_PATH_LENGTH);
}

/**
 * Given an existing, but unsorted, DirEntryIndex, sort the entries.
 */
void dei_reindex(DirEntryIndex *dei, DirData *dd) {
    srfsLog(LOG_FINE, "dei_reindex %llx %llx", dei, dd);
    dei_sanity_check(dei);
    dd_sanity_check(dd);
#ifdef USE_QSORT_R
    qsort_r(dei->entries, dei->numEntries, sizeof(DEIndexEntry), dei_comp, dd);
#else
    thread_context = dd;
    qsort(dei->entries, dei->numEntries, sizeof(DEIndexEntry), dei_comp_old);
#endif
}

void dei_add_numEntries_and_reindex(DirEntryIndex *dei, DirData *dd, uint32_t numNewEntries) {
    srfsLog(LOG_FINE, "dei_add_numEntries_and_reindex %llx %llx %d", dei, dd, numNewEntries);
    dei_sanity_check(dei);
    dd_sanity_check(dd);
    dei->numEntries += numNewEntries;
    dei_reindex(dei, dd);
}

/**
 * Given a DirEntryIndex, and an index entry number return the DirEntry
 * in the given DirData.
 */
DirEntry *dei_get_dir_entry(DirEntryIndex *dei, DirData *dd, uint32_t index, int fatalErrorOnFailure) {
    DirEntry    *de;
    int rc;
    
    rc = dei_sanity_check(dei, fatalErrorOnFailure);
    if (rc != TRUE) {
        return NULL;
    }
    rc = dd_sanity_check(dd, fatalErrorOnFailure);
    if (rc != TRUE) {
        return NULL;
    }
    de = (DirEntry *)offset_to_ptr(dd->data, dei->entries[index]);
    if ((uint64_t)de > (uint64_t)dd->data + (uint64_t)dd->dataLength) {
        srfsLog(LOG_ERROR, "de > dd->data + dd->dataLength %s %d", __FILE__, __LINE__);
        if (fatalErrorOnFailure) {
            fatalError("de > dd->data + dd->dataLength");
        }
        return NULL;
    }
    rc = de_sanity_check(de, fatalErrorOnFailure);
    if (rc != TRUE) {
        return NULL;
    }
    return de;
}

uint32_t dei_locate(DirEntryIndex *dei, DirData *dd, const char *name) {
    uint32_t    lower;
    uint32_t    upper;

    srfsLog(LOG_FINE, "dei_locate %llx %llx %s", dei, dd, name);
    dei_sanity_check(dei);
    
    if (dei->numEntries == 0) {
        srfsLog(LOG_FINE, "dei->numEntries == 0");
        return DEI_NOT_FOUND;
    }
    
    lower = 0;
    upper = dei->numEntries - 1;
    while (TRUE) {
        uint32_t    range;
        int         cmp;
        
        range = upper - lower;
        if (range < 2) {
            DirEntry    *de;
            
            srfsLog(LOG_FINE, "range < 2");
            if (range == 0) {
                srfsLog(LOG_FINE, "range == 0");
                cmp = strncmp(name, de_get_name(dei_get_dir_entry(dei, dd, lower)), SRFS_MAX_PATH_LENGTH);
                srfsLog(LOG_FINE, "name %s cmp %d", name, cmp);
                if (cmp == 0) {
                    return lower;
                } else {
                    return DEI_NOT_FOUND;
                }
            } else { // range == 1
                cmp = strncmp(name, de_get_name(dei_get_dir_entry(dei, dd, lower)), SRFS_MAX_PATH_LENGTH);
                if (cmp < 0) {
                    return DEI_NOT_FOUND;
                } else if (cmp == 0) {
                    return lower;
                } else { // (cmp > 0)
                    cmp = strncmp(name, de_get_name(dei_get_dir_entry(dei, dd, upper)), SRFS_MAX_PATH_LENGTH);
                    if (cmp < 0) {
                        return DEI_NOT_FOUND;
                    } else if (cmp == 0) {
                        return upper;
                    } else { // (cmp > 0)
                        return DEI_NOT_FOUND;
                    }
                }
            }
        } else {
            uint32_t    middle;
            
            middle = lower + range / 2;
            cmp = strncmp(name, de_get_name(dei_get_dir_entry(dei, dd, middle)), SRFS_MAX_PATH_LENGTH);
            if (cmp < 0) {
                upper = middle - 1;
            } else if (cmp > 0) {
                lower = middle + 1;
            } else {// (cmp == 0)
                return middle;
            }
        }
    }
}
