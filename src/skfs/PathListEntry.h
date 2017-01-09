// PathListEntry.h

#ifndef _PATH_LIST_ENTRY_H_
#define _PATH_LIST_ENTRY_H_

//////////
// types

typedef struct PathListEntry {
	char			*path;
	PathListEntry	*next;
} PathListEntry;


PathListEntry *ple_prepend(PathListEntry *existingEntry, char *path);

#endif

