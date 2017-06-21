// DirData.c

/////////////
// includes

#include "DirData.h"
#include "DirEntry.h"
#include "DirEntryIndex.h"
#include "SRFSConstants.h"
#include "Util.h"

#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>


/////////////////////
// private defines

#define DD_CHECK_ALL_ENTRIES_ON_FULL FALSE


///////////////////////
// private prototypes

static DirEntryIndex *dd_get_index(DirData *dd);


////////////////////////////
// DirData implementation

static void dd_debug(DirData *dd, char *f, int line) {
    srfsLog(LOG_FINE, "%s %d %x %x %x %d %d", f, line, dd, dd_get_index(dd), dd->magic, dd->magic, dd->indexOffset);
}

/**
 * Create an empty DirData instance
 */
DirData *dd_new_empty() {
	DirData	*dd;
	size_t	dd_length_no_header;
	size_t	dd_length_no_header_no_index;
	size_t	dd_length_with_header_and_index;
    DirEntryIndex   *dei;
			
	dd_length_no_header_no_index = 0;
	dd_length_with_header_and_index = dd_length_no_header_no_index + DD_HEADER_BYTES + DEI_HEADER_BYTES;
    dd_length_no_header = dd_length_no_header_no_index + DEI_HEADER_BYTES;
	
	dd = (DirData*)mem_alloc(1, dd_length_with_header_and_index);
	dd->magic = DD_MAGIC;
	dd->dataLength = dd_length_no_header;
    dei = (DirEntryIndex *)dd->data;
    dei_init(dei, 0);
	dd->indexOffset = ptr_to_offset(dd->data, dei);
	dd->numEntries = 0;
    dei_init(dei, 0);
    dei_sanity_check(dei);
    dei_sanity_check(dd_get_index(dd));
    //dd_debug(dd, __FILE__, __LINE__);
	return dd;
}

void dd_delete(DirData **dd) {
	if (dd != NULL && *dd != NULL) {
		if ((*dd)->magic == DD_MAGIC) {
			(*dd)->magic = 0;
			mem_free((void **)dd);
		} else {
			fatalError("bad magic in dd_delete");
		}
	} else {
		fatalError("bad ptr in dd_delete");
	}
}

int dd_sanity_check(DirData *dd, int fatalErrorOnFailure) {
	if (dd->magic != DD_MAGIC) {
        if (fatalErrorOnFailure) {
            srfsLog(LOG_ERROR, "dd->magic != DD_MAGIC  dd %llx dd->magic %x != %x", dd, dd->magic, DD_MAGIC);
			fatalError("dd->magic != DD_MAGIC", __FILE__, __LINE__);
		}
		return FALSE;
	} else {
		return TRUE;
	}
}

int dd_sanity_check_full(DirData *dd, int fatalErrorOnFailure) {
    if (dd_sanity_check(dd, fatalErrorOnFailure)) {
        DirEntryIndex   *dei;
        uint32_t i;
        
        dei = dd_get_index(dd);
        dei_sanity_check(dei);
        if (dd->numEntries != dei->numEntries) {
            if (fatalErrorOnFailure) {
                fatalError("dd->numEntries != dei->numEntries", __FILE__, __LINE__);
            } else {
                return FALSE;
            }
        }
        if (DD_CHECK_ALL_ENTRIES_ON_FULL) {
            // sanity check each entry
            for (i = 0; i < dd->numEntries; i++) {
                de_sanity_check(dei_get_dir_entry(dei, dd, i), fatalErrorOnFailure);
            }
        } else {
            // spot check first and last entries
            if (dei->numEntries > 0) {
                de_sanity_check(dei_get_dir_entry(dei, dd, 0), fatalErrorOnFailure);
                if (dei->numEntries > 1) {
                    de_sanity_check(dei_get_dir_entry(dei, dd, dei->numEntries - 1), fatalErrorOnFailure);
                }
            }
        }
        return TRUE;
    } else {
        return FALSE;
    }
}

size_t dd_length_with_header_and_index(DirData *dd) {
	return DD_HEADER_BYTES + dd->dataLength;
}

DirData *dd_fuse_fi_fh_to_dd(struct fuse_file_info *fi) {
	DirData	*dd;
	
	dd = (DirData *)fi->fh;
	if (dd != NULL) {
		if (dd->magic != DD_MAGIC) {
			srfsLog(LOG_ERROR, "Bogus fi->fh. dd->magic is %x", dd->magic);			
			dd = NULL;
		}
	}
	return dd;
}

static DirEntryIndex *dd_get_index(DirData *dd) {
    DirEntryIndex   *dei;
    
    dei = (DirEntryIndex *)offset_to_ptr(dd->data, dd->indexOffset);
    dei_sanity_check(dei);
    return dei;
}

DirEntry *dd_get_entry(DirData *dd, uint32_t index) {
    return dei_get_dir_entry(dd_get_index(dd), dd, index);
}

DirEntry *dd_get_entry_by_name(DirData *dd, const char *name) {
    uint32_t        index;
    DirEntryIndex   *dei;
    
    dei = dd_get_index(dd);
    index = dei_locate(dei, dd, name);
    if (index != DEI_NOT_FOUND) {
        return dei_get_dir_entry(dei, dd, index);
    } else {
        return NULL;
    }
}

int dd_contains(DirData *dd, char *name) {
	DirEntry	*de;
	
    //dd_debug(dd, __FILE__, __LINE__);
	de = dd_get_entry_by_name(dd, name);
	return de != NULL;
}

static void dd_updates_mem_required(DirData *dd, OpenDirUpdate *updates, int numUpdates, 
                                    uint32_t *uniqueUpdates,
									uint64_t *deAdditionalMemRequired, uint64_t *indexAdditionalMemRequired) {
	int i;
	
    *uniqueUpdates = 0;
	*deAdditionalMemRequired = 0;
	*indexAdditionalMemRequired = 0;
	for (i = 0; i < numUpdates; i++) {
		if (!dd_contains(dd, updates[i].name)) {
            ++(*uniqueUpdates);
			*deAdditionalMemRequired += de_compute_total_size_from_name(updates[i].name);
			*indexAdditionalMemRequired += DEI_ENTRY_SIZE;
		}
	}
}

static uint64_t dd_index_size_bytes(DirData *dd) {
	return dd->dataLength - dd->indexOffset;
}

DirData *dd_process_updates(DirData *dd, OpenDirUpdate *updates, int numUpdates) {
    srfsLog(LOG_FINE, "dd_process_updates %llx %llx %d", dd, updates, numUpdates);
    //dd_debug(dd, __FILE__, __LINE__);
    // FUTURE: plenty of room for improvement
	if (numUpdates == 0) {
		srfsLog(LOG_FINE, "numUpdates is 0, dd_dup");
		return dd_dup(dd);
	} else {
		DirData	*_dd;
        uint32_t    uniqueUpdates;
		uint64_t	deAdditionalMemRequired;
		uint64_t	indexAdditionalMemRequired;
		uint64_t	totalAdditionalMemRequired;
		int	i;
		
		// 1)
		// Determine how much additional memory is required
		// Allocate new block of memory
		// Copy existing data to new location
			// modified header
			// entries to data[]
			// index to new index location
		// Add all new entries
			// New DirEntry
			// New DEIndexEntry
		// Reindex
		
		// Determine how much additional memory is required		
		deAdditionalMemRequired = 0;
		indexAdditionalMemRequired = 0;
		dd_updates_mem_required(dd, updates, numUpdates, &uniqueUpdates, &deAdditionalMemRequired, &indexAdditionalMemRequired);
		totalAdditionalMemRequired = deAdditionalMemRequired + indexAdditionalMemRequired;
        
        srfsLog(LOG_FINE, "memreq %u %u %u %u", uniqueUpdates, deAdditionalMemRequired, indexAdditionalMemRequired, totalAdditionalMemRequired);
		
		// Allocate new block of memory
		_dd = (DirData *)mem_alloc(dd_length_with_header_and_index(dd) + totalAdditionalMemRequired, 1);
		
		// Copy existing data to new location
			// modified header
		_dd->magic = DD_MAGIC;
		_dd->dataLength = dd->dataLength + totalAdditionalMemRequired;
		_dd->indexOffset = dd->indexOffset + deAdditionalMemRequired;
		_dd->numEntries = dd->numEntries + uniqueUpdates;
        srfsLog(LOG_FINE, "dd->dataLength %d dd->indexOffset %d dd->numEntries %d", 
            dd->dataLength, dd->indexOffset, dd->numEntries);
        srfsLog(LOG_FINE, "_dd->dataLength %d _dd->indexOffset %d _dd->numEntries %d", 
            _dd->dataLength, _dd->indexOffset, _dd->numEntries);
		
			// entries to data[]
		memcpy((void *)_dd->data, (void *)dd->data, dd->indexOffset);

			// index to new index location
		memcpy(offset_to_ptr(_dd->data, _dd->indexOffset), offset_to_ptr(dd->data, dd->indexOffset), dd_index_size_bytes(dd));
        dei_sanity_check((DirEntryIndex *)offset_to_ptr(dd->data, dd->indexOffset));
        dei_sanity_check((DirEntryIndex *)offset_to_ptr(_dd->data, _dd->indexOffset));
		
		// Add all new entries, modify existing
		DirEntry    *nextDE;
		DEIndexEntry    *nextIndexEntry;
		uint32_t    numNewEntriesAdded;
		
        // start creating new DirEntries at the end of the existing list
		nextDE = (DirEntry *)offset_to_ptr(_dd->data, dd->indexOffset);
        // start creating new DEIndexEntries at the end of the existing index
		nextIndexEntry = (DEIndexEntry *)offset_to_ptr(_dd->data, _dd->indexOffset + dd->numEntries * DEI_ENTRY_SIZE + DEI_HEADER_BYTES);
		numNewEntriesAdded = 0;
		
		for (i = 0; i < numUpdates; i++) {
			DirEntry	*prevDE;
		
            dei_sanity_check((DirEntryIndex *)offset_to_ptr(_dd->data, _dd->indexOffset));
            // locate an existing entry using the old dd
            // (Can't use new since the index is incomplete)
			prevDE = dd_get_entry_by_name(dd, updates[i].name);
			if (prevDE == NULL) {
                DirEntry    *checkEntry;
                
				++numNewEntriesAdded;
				// New DEIndexEntry
				nextIndexEntry = deie_set(nextIndexEntry, _dd, nextDE);
				// New DirEntry
                srfsLog(LOG_FINE, "filling nextDE %x", nextDE);
                checkEntry = nextDE;
				nextDE = de_init_from_update(nextDE, &updates[i]);
                de_sanity_check(checkEntry);
                dei_sanity_check((DirEntryIndex *)offset_to_ptr(_dd->data, _dd->indexOffset));
			} else {
                DirEntry	*_prevDE;
            
                srfsLog(LOG_FINE, "updating prevDE %x", prevDE);
                de_sanity_check(prevDE);
                // Find the corresponding entry in the new DirData
                _prevDE = (DirEntry *)offset_to_ptr(_dd->data, ptr_to_offset(dd->data, prevDE));
                de_sanity_check(_prevDE);
				// modify existing entry
				if (updates[i].version > _prevDE->version) {
					de_update(_prevDE, &updates[i]);
				} else {
					// Ignoring stale update
				}
                de_sanity_check(_prevDE);
                dei_sanity_check((DirEntryIndex *)offset_to_ptr(_dd->data, _dd->indexOffset));
			}
            dei_sanity_check((DirEntryIndex *)offset_to_ptr(_dd->data, _dd->indexOffset));
		}
        dei_sanity_check((DirEntryIndex *)offset_to_ptr(_dd->data, _dd->indexOffset));

        // Sanity check pointers after construction
        if (numNewEntriesAdded != uniqueUpdates) {
            fatalError("numNewEntriesAdded != uniqueUpdates", __FILE__, __LINE__);
        }
        if ((void *)nextIndexEntry != offset_to_ptr(_dd->data, _dd->dataLength)) {
            fatalError("(void *)nextIndexEntry != offset_to_ptr(_dd->data, _dd->dataLength)", __FILE__, __LINE__);
        }
		
		// Reindex
		dei_add_numEntries_and_reindex(dd_get_index(_dd), _dd, numNewEntriesAdded);
		
		return _dd;
	}
}

MergeResult dd_merge(DirData *dd0, DirData *dd1) {
	uint32_t    dd1Size;
	uint32_t    dd2Size;
	MergeResult m;
	DirData     *tdd;
    uint32_t    tddNumEntries;
    DirEntry    *nextDE;
	uint32_t    i0;
	uint32_t    i1;
    DEIndexEntry    *tmpIndex;
    DEIndexEntry    *nextIndexEntry;

    dd_sanity_check_full(dd0);
    dd_sanity_check_full(dd1);
    srfsLog(LOG_FINE, "merge %llx %llx", dd0, dd1);
    srfsLog(LOG_FINE, "%d %d", dd0->numEntries, dd1->numEntries);

	memset(&m, 0, sizeof(MergeResult));	
	i0 = 0;
	i1 = 0;
    tddNumEntries = 0;
	
	// Allocate temporary space for working
	tdd = (DirData *)mem_alloc(dd_length_with_header_and_index(dd0) + dd_length_with_header_and_index(dd1), 1);
    nextDE = (DirEntry *)tdd->data;
    tmpIndex = (DEIndexEntry *)mem_alloc(dd0->numEntries + dd1->numEntries, sizeof(DEIndexEntry));
    //tmpIndex = (DEIndexEntry *)offset_to_ptr(tdd->data, dd0->indexOffset + dd1->indexOffset);
	nextIndexEntry = tmpIndex;
    
    // Merge entries until end of one DirData reached
	while (i0 < dd0->numEntries && i1 < dd1->numEntries) {
		DirEntry	*de0;
		DirEntry	*de1;
		int			cmp;
		
        // As we go along, we construct a proto-index
        nextIndexEntry = deie_set(nextIndexEntry, tdd, nextDE);
        
        // Note that we use the existing indices to get an ordered view of the entries
        // in both DirDatas. As we store the new entries in the order found, 
        // this has the side effect of sorting the actual entries in the 
        // resulting merged data.
        
		de0 = dd_get_entry(dd0, i0);
		de1 = dd_get_entry(dd1, i1);
		cmp = strncmp(de_get_name(de0), de_get_name(de1), SRFS_MAX_PATH_LENGTH);
		if (cmp < 0) {
			// add entry 0
            nextDE = de_init_from_de(nextDE, de0);
            m.dd0NotIn1 = TRUE;
            ++i0;
		} else if (cmp > 0) {
			// add entry 1
            nextDE = de_init_from_de(nextDE, de1);
            m.dd1NotIn0 = TRUE;
            ++i1;
		} else {
			// matching entry, add newer
			if (de0->version > de1->version) {
				// add de0
                nextDE = de_init_from_de(nextDE, de0);
                m.dd0NotIn1 = TRUE;
			} else if (de0->version < de1->version) {
				// add de1
                nextDE = de_init_from_de(nextDE, de1);
                m.dd1NotIn0 = TRUE;
			} else {
                FileStatus  fs;
            
                // equal versions, add arbitrary, but check deleted status
                memset(&fs, 0, sizeof(FileStatus));
				if (!fs_get_deleted(&de0->status) || !fs_get_deleted(&de1->status)) {
					// one not deleted => not deleted
                    if (srfsLogLevelMet(LOG_FINE)) {
                        srfsLog(LOG_FINE, "fs_set_deleted %s FALSE", de_get_name(de0));
                    }
                    fs_set_deleted(&fs, FALSE);
				} else {
					// both deleted, set status to deleted
                    if (srfsLogLevelMet(LOG_FINE)) {
                        srfsLog(LOG_FINE, "fs_set_deleted %s TRUE", de_get_name(de0));
                    }
                    fs_set_deleted(&fs, TRUE);
				}
                nextDE = de_init(nextDE, de0->dataSize, fs, de0->version, de0->data);
			}
            ++i0;
            ++i1;
		}
        ++tddNumEntries;
        srfsLog(LOG_FINE, "%d %d %d", i0, i1, tddNumEntries);
	}
    
    // Add any remaining entries 
    // (There can only be one DirData with entries that have not been added)
    uint32_t    i;
    DirData     *idd;
    DirEntry    *ide;
    
	if (i0 < dd0->numEntries) {
        if (i1 < dd1->numEntries) {
            fatalError("i1 < dd1->numEntries", __FILE__, __LINE__);
        }
        i = i0;
        idd = dd0;
        m.dd0NotIn1 = TRUE;
    } else if (i1 < dd1->numEntries) {
        i = i1;
        idd = dd1;
        m.dd1NotIn0 = TRUE;
    } else {
        // both i0 and i1 past, no additions needed
        idd = dd0; // arbitrarily use dd0
        i = idd->numEntries; // sent termination condition on loop
    }
    while (i < idd->numEntries) {
        nextIndexEntry = deie_set(nextIndexEntry, tdd, nextDE);
        ide = dd_get_entry(idd, i);
        nextDE = de_init_from_de(nextDE, ide);
        i++;
        ++tddNumEntries;
    }
    // We have now added all DirEntries.
    // Also, we have created a complete, but unsorted index    
    
    if (!m.dd1NotIn0) {
        // Nothing new in d1. Ignore this merge.
        m.dd = NULL;
    } else {
        // Useful merge. Finish construction.
    
        // Set tdd header
        tdd->magic = DD_MAGIC;
        tdd->dataLength = ptr_to_offset(tdd->data, ((char *)nextDE) + DEI_HEADER_BYTES + tddNumEntries * sizeof(DEIndexEntry));
        tdd->numEntries = tddNumEntries;
        
        // Create the index
        DirEntryIndex   *tDEI;
        
        tDEI = (DirEntryIndex *)nextDE;
        tdd->indexOffset = ptr_to_offset(tdd->data, tDEI);
        memcpy((void *)&tDEI->entries, (void *)tmpIndex, tddNumEntries * sizeof(DEIndexEntry));
        dei_init(tDEI, tddNumEntries);
        dei_reindex(tDEI, tdd);
        //dei_add_numEntries_and_reindex(tDEI, tdd, tddNumEntries);
        
        // Copy data from temp space to correctly sized buffer
        m.dd = (DirData *)mem_alloc(dd_length_with_header_and_index(tdd), 1);
        memcpy(m.dd, tdd, dd_length_with_header_and_index(tdd));        
        
        dd_sanity_check_full(m.dd);
    }
    
    // Free temp space
    mem_free((void **)&tdd);
    mem_free((void **)&tmpIndex);
    
    srfsLog(LOG_FINE, "MergeResult %llx %d %d", m.dd, m.dd0NotIn1, m.dd1NotIn0);
    
    return m;
}

DirData *dd_dup(DirData *dd) {
	return (DirData *)mem_dup(dd, dd_length_with_header_and_index(dd));
}

void dd_display(DirData *dd, FILE *file) {
	if (dd == NULL) {
		fprintf(file, "DirData {NULL}\n");
	} else {
		DirEntry	*de;
		void		*limit;
	
		fprintf(file, "DirData {\n");
        limit = offset_to_ptr(dd->data, dd->indexOffset);
		de = de_initial(dd->data, (DirEntry *)limit);
		printf("de_1 %llx\n", (unsigned long long)de);
		while (de) {
			de_display(de);
			de = de_next(de, (DirEntry *)limit);
			printf("de_2 %llx\n", (unsigned long long)de);
		}
		fprintf(file, "}\n\n");
	}
}

void dd_display_ordered(DirData *dd, FILE *file) {
	if (dd == NULL) {
		fprintf(file, "DirData {NULL}\n");
	} else {
		DirEntry	*de;
		void		*limit;
        uint32_t    i;
	
		fprintf(file, "DirData {\n");
        for (i = 0; i < dd->numEntries; i++) {
            de = dd_get_entry(dd, i);
			de_display(de);
		}
		fprintf(file, "}\n\n");
	}
}

int dd_is_empty(DirData *dd) {
    uint32_t    i;

    for (i = 0; i < dd->numEntries; i++) {
        DirEntry	*de;
        
        de = dd_get_entry(dd, i);
        if (!de_is_deleted(de)) {
            return FALSE;
        }
    }
    return TRUE;
}
