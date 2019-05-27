// PathGroup.c

/////////////
// includes

#include "PathGroup.h"
#include "SRFSConstants.h"
#include "Util.h"

#include <string.h>


//////////
// Types

typedef struct PathGroupEntry {
    char    *path;
    int        length;
    int        applyToChildren;
} PathGroupEntry;


//////////////////
//Implementation

// PathGroupEntry

static PathGroupEntry *pge_new(char *path, int applyToChildren) {
    PathGroupEntry    *pge;

    pge = (PathGroupEntry *)mem_alloc(1, sizeof(PathGroupEntry));
    pge->path = str_dup(path);
    pge->length = strlen(path);
    pge->applyToChildren = applyToChildren;
    return pge;
}

static void pge_delete(PathGroupEntry **pge) {
    if (pge != NULL && *pge != NULL) {
        mem_free((void **)&((*pge)->path));
        mem_free((void **)pge);
    } else {
        fatalError("bad ptr in pge_delete");
    }
}

static int pge_matches(PathGroupEntry *pge, const char *path, int compLength, int strict) {
    if (pge->applyToChildren) {
        return !strncmp(pge->path, path, pge->length);
    } else {
        if (!strict) {
            compLength = int_max(compLength, pge->length);
        }
        srfsLog(LOG_FINE, "pge_matches: %s %s %d", pge->path, path, pge->length);
        return !strncmp(pge->path, path, compLength);
    }
}

static void pge_to_string(PathGroupEntry * pge, char *dest) {
    sprintf(dest, "%s:%u:%u", pge->path, pge->length, pge->applyToChildren);
}


// PathGroup

PathGroup *pg_new(char *name, int compareDirectoriesOnly) {
    PathGroup    *pg;

    pg = (PathGroup *)mem_alloc(1, sizeof(PathGroup));
    pg->name = str_dup(name);
    pg->compareDirectoriesOnly = compareDirectoriesOnly;
    return pg;
}

void pg_delete(PathGroup **pg) {
    if (pg != NULL && *pg != NULL) {
        int    i;

        for (i = 0; i < (*pg)->size; i++) {
            pge_delete(&(*pg)->entries[i]);
        }
        mem_free((void **)&((*pg)->name));
        mem_free((void **)pg);
    } else {
        fatalError("bad ptr in pg_delete");
    }
}

void pg_add_entry(PathGroup *pg, char *path) {
    PathGroupEntry    *pge;
    char            *loc;
    char            *tmpPath;
    int                applyToChildren;

    tmpPath = str_dup(path);
    loc = strchr(tmpPath, PG_DELIMITER);
    if (loc != NULL) {
        char    *applyToChildrenDef;

        *loc = '\0';
        applyToChildrenDef = loc + 1;
        if (strlen(applyToChildrenDef) > 0) {
            applyToChildren = *applyToChildrenDef == 'y' || *applyToChildrenDef == 'Y';
        } else {
            applyToChildren = FALSE;
        }
    } else {
        applyToChildren = FALSE;
    }
    pge = pge_new(tmpPath, applyToChildren);
    if (pge->length != 0) {
        pg->entries[pg->size] = pge;
        pg->size++;
    } else {
        pge_delete(&pge);
    }
    mem_free((void **)&tmpPath);
}

int pg_matches(PathGroup volatile *pg, const char *path, int compLength) {
    int        i;
    int        strict;

    srfsLog(LOG_FINE, "pg_matches, %s %d", path, compLength);
    if (compLength == 0) {
        strict = FALSE;
        if (pg->compareDirectoriesOnly) {
            const char    *lastSlash;

            lastSlash = strrchr(path, '/');
            if (lastSlash != NULL) {
                compLength = (int)(lastSlash - path);
            } else {
                compLength = strlen(path);
            }
        } else {
            compLength = strlen(path);
        }
    } else {
        strict = TRUE;
    }
    for (i = 0; i < pg->size; i++) {
        if (pge_matches(pg->entries[i], path, compLength, strict)) {
            return TRUE;
        }
    }
    return FALSE;
}

int pg_size(PathGroup *pg) {
    return pg->size;
}

char *pg_get_member(PathGroup *pg, int index) {
    if (index >= 0 && index < pg->size) {
        return pg->entries[index]->path;
    } else {
        return NULL;
    }
}

void pg_display(PathGroup *pg) {
    char    pgeBuf[SRFS_MAX_PATH_LENGTH];
    int        i;

    srfsLog(LOG_WARNING, "%s %d", __FILE__, __LINE__);
    srfsLog(LOG_WARNING, "%s", pg->name);
    for (i = 0; i < pg->size; i++) {
        pge_to_string(pg->entries[i], pgeBuf);
        srfsLog(LOG_WARNING, "%d\t%s", i, pgeBuf);
    }
}

void pg_parse_paths(PathGroup *pg, char *paths) {
    char        *cur;
    char        *div;
    char        *next;

    cur = (char *)paths;
    while (cur != NULL) {
        div = strchr(cur, ',');
        if (div == NULL) {
            next = NULL;
        } else {
            next = div + 1;
            *div = '\0';
        }
        pg_add_entry(pg, cur);
        cur = next;
    }
    pg_display(pg);
}
