// DirEntry.c

/////////////
// includes

#include "DirEntry.h"


////////////
// defines

//#define DE_DEBUG

////////////////////////////
// DirEntry implementation

int de_compute_total_size_from_name_length(int length) {
    int totalSize;
    
    // the string characters + terminator + header
    totalSize = length + 1 + DE_HEADER_BYTES;
    if ((totalSize % 2) == 1) {
        ++totalSize;
    }
    return totalSize;
}

int de_compute_total_size_from_name(char *s) {
    return de_compute_total_size_from_name_length(strnlen(s, SRFS_MAX_PATH_LENGTH));
}

int de_compute_data_size_from_name_length(int length) {
    int totalSize;
    
    // the string characters + terminator
    totalSize = length + 1;
    if ((totalSize % 2) == 1) {
        ++totalSize;
    }
    return totalSize;
}

int de_compute_data_size_from_name(char *s) {
    return de_compute_data_size_from_name_length(strnlen(s, SRFS_MAX_PATH_LENGTH));
}

DirEntry *de_init(DirEntry *de, int dataSize, FileStatus status, uint64_t version, char *data) {
    ensure_null_terminated(data, data + dataSize, __FILE__, __LINE__);
    de->magic = DE_MAGIC;
    de->dataSize = dataSize;
    de->status = status;
    de->version = version;
    memcpy(de->data, data, dataSize);
    return (DirEntry *)(de->data + dataSize);
}

DirEntry *de_init_from_de(DirEntry *de, DirEntry *oDE) {
    return de_init(de, oDE->dataSize, oDE->status, oDE->version, oDE->data);
}

DirEntry *de_init_from_update(DirEntry *de, OpenDirUpdate *odu) {
    FileStatus fs;
    
    switch (odu->type) {
    case ODU_T_ADDITION:
        fs_set_deleted(&fs, FALSE);
        break;
    case ODU_T_DELETION:
        fs_set_deleted(&fs, TRUE);
        break;
    default:
        fatalError("Panic", __FILE__, __LINE__);
    }
    return de_init(de, de_compute_data_size_from_name(odu->name), fs, odu->version, odu->name);
}

/**
 * Begin iteration through directory entries by searching for 
 * a DirEntry in a blob of memory.
 */
DirEntry *de_initial(const char *blob, DirEntry *limit) {
    DirEntry    *de;
    int            sane;
    
#ifdef DE_DEBUG
    fprintf(stderr, "blob %llx limit %llx\n", blob, limit);
#endif
    if (blob >= (const char *)limit) {
#ifdef DE_DEBUG
        fprintf(stderr, "blob >= limit\n");
#endif
        return NULL;
    }
    ensure_null_terminated((char *)blob, (char *)limit, __FILE__, __LINE__);
    de = (DirEntry *)blob;
    sane = de_sanity_check(de, FALSE);
    while (!sane) {
        // FUTURE - remove this search
        if ((uint64_t)de >= (uint64_t)limit) {
            srfsLog(LOG_FINE, "Unable to find valid DirEntry from %llx", blob);
            return NULL;
        }
        de = (DirEntry *)(((unsigned char *)de) + 1);
        srfsLog(LOG_FINE, "Searching for valid DirEntry at %llx", de);
        sane = de_sanity_check(de, FALSE);
    }
    return de;
}

int de_sanity_check(DirEntry *de, int fatalErrorOnFailure) {
    if (de->magic != DE_MAGIC) {
        srfsLog(LOG_ERROR, "de->magic != DE_MAGIC  de %llx de->magic %x != %x", de, de->magic, DE_MAGIC);
        if (fatalErrorOnFailure) {
            fatalError("de->magic != DE_MAGIC", __FILE__, __LINE__);
        }
        return FALSE;
    } else {
        return TRUE;
    }
}
    
DirEntry *de_next(DirEntry *de, DirEntry *limit, int sanityCheck) {
    DirEntry    *next;
    
    next = (DirEntry *)(de->data + de->dataSize);
    if (next >= limit) {
        return NULL;
    } else {
        if (sanityCheck) {
            de_sanity_check(next);
            if (next->dataSize == 0) {
                return NULL;
            } else {
                return next;
            }
        } else {
            return next;
        }
    }
}

int de_is_deleted(DirEntry *de) {
    return fs_get_deleted(&de->status);
}

const char *de_get_name(DirEntry *de) {
    return de->data;
}

void de_display(DirEntry *de, FILE *file) {
    if (de) {
        //printf("%llx\n", de);
        fprintf(file, "\t%x %d %s %s\n", de->magic, de->dataSize, de->data, fs_to_string(&de->status)); fflush(0);
    } else {
        fprintf(file, "NULL DirEntry\n"); fflush(0);
    }
}

void de_update(DirEntry *de, OpenDirUpdate *odu) {
    if (srfsLogLevelMet(LOG_FINE)) {
        srfsLog(LOG_FINE, "de_update %llx %s %d", de, de_get_name(de), odu->type);
    }
    switch (odu->type) {
    case ODU_T_ADDITION:
        fs_set_deleted(&de->status, FALSE);
        break;
    case ODU_T_DELETION:
        fs_set_deleted(&de->status, TRUE);
        break;
    default:
        fatalError("Panic", __FILE__, __LINE__);
    }
    de->version = odu->version;
    if (srfsLogLevelMet(LOG_FINE)) {
        srfsLog(LOG_FINE, "%llx %s %d %d", de, de_get_name(de), de->version, fs_get_deleted(&de->status));
    }
}
