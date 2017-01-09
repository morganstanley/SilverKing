// PathList.h

#ifndef _PATH_GROUP_H_
#define _PATH_GROUP_H_

/////////////
// includes

#include "Util.h"


////////////
// defines

#define PG_MAX_SIZE	1024
#define PG_DELIMITER ':'


//////////
// Types

struct PathGroupEntry;

typedef struct PathGroup {
	char			*name;
	int				compareDirectoriesOnly;
	int				size;
	PathGroupEntry	*(entries[PG_MAX_SIZE]);
} PathGroup;


//////////////////////
// public prototypes

PathGroup *pg_new(char *name, int compareDirectoriesOnly = TRUE);
void pg_delete(PathGroup **pg);
void pg_add_entry(PathGroup *pg, char *path);
int pg_matches(PathGroup volatile *pg, const char *path, int compLength = 0);
void pg_display(PathGroup *pg);
void pg_parse_paths(PathGroup *pg, char *paths);
int pg_size(PathGroup *pg);
char *pg_get_member(PathGroup *pg, int index);

#endif
