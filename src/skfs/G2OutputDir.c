// G2OutputDir.c

/////////////
// includes

#include "G2OutputDir.h"
#include "Util.h"

#include <string.h>
#include <sys/stat.h>


////////////////////
// private defines

#define TASK_ID_DELIMITER '\n'


//////////////////
// private types

static OutputDir *g2od_new(OutputDirEntry **entries, int numEntries);


///////////////////////
// private prototypes


////////////////////
// private members


///////////////////
// OutputDirEntry

static OutputDirEntry *ode_new(char	*name, int nameSize, ino_t ino) {
	OutputDirEntry	*ode;

	ode = (OutputDirEntry *)mem_alloc(1, sizeof(OutputDirEntry));
	ode->name = name;
	ode->nameSize = nameSize;
	ode->ino = ino;
	ode->outLength = G2OD_LENGTH_NOT_SET;
	ode->errLength = G2OD_LENGTH_NOT_SET;
	return ode;
}

static void ode_delete(OutputDirEntry **ode) {
	if (ode != NULL && *ode != NULL) {
		if ((*ode)->hostname != NULL) {
			mem_free((void **)(*ode)->hostname);
		}
		mem_free((void **)ode);
	} else {
		fatalError("bad ptr in ode_delete");
	}
}


//////////////
// OutputDir

static OutputDir *g2od_new(OutputDirEntry **entries, int numEntries) {
	OutputDir	*od;

	od = (OutputDir *)mem_alloc(1, sizeof(OutputDir));
	od->entries = entries;
	od->numEntries = numEntries;
    pthread_rwlock_init(&od->rwLock, 0); 
	return od;
}

void g2od_delete(OutputDir **od) {
	if (od != NULL && *od != NULL) {
		if ((*od)->numEntries > 0) {
			int	i;

			for (i = 0; i < (*od)->numEntries; i++) {
				ode_delete(&(*od)->entries[i]);
			}
		}
		pthread_rwlock_destroy(&(*od)->rwLock);
		mem_free((void **)od);
	} else {
		fatalError("bad ptr in g2od_delete");
	}
}

OutputDir *g2od_parse(char *def, int length) {
	OutputDirEntry	**entries;
	int	numEntries;
	int	i;

	numEntries = 0;
	for (i = 0; i < length; i++) {
		if (def[i] == TASK_ID_DELIMITER) {
			++numEntries;
		}
	}
	if (numEntries > 0) {
		char	*tokStart;

		entries = (OutputDirEntry **)mem_alloc(numEntries, sizeof(OutputDirEntry *));
		tokStart = def;
		for (i = 0; i < numEntries; i++) {
			char	*tokEnd;
			char	*taskID;
			int		taskIDSize;
			ino_t	ino;

			tokEnd = strchr(tokStart, TASK_ID_DELIMITER);
			*tokEnd = '\0';
			taskID = str_dup(tokStart);
			taskIDSize = tokEnd - tokStart + 1;
			ino = (ino_t)stringHash(taskID);
			entries[i] = ode_new(taskID, taskIDSize, ino);
			tokStart = tokEnd + 1;
		}
	} else {
		entries = NULL;
	}
	return g2od_new(entries, numEntries);
}

void g2od_display(OutputDir *od) {
	int	i;

	for (i = 0; i < od->numEntries; i++) {
		printf("%d\t%s\n", i, od->entries[i]->name);
	}
}

static int g2od_index_of(OutputDir *od, char *taskID) {
	int	i;

	for (i = 0; i < od->numEntries; i++) {
		if (!strcmp(od->entries[i]->name, taskID)) {
			return i;
		}
	}
	return -1;
}

char *g2od_get_hostname(OutputDir *od, char *taskID) {
	int		i;

	i = g2od_index_of(od, taskID);
	if (i >= 0) {
		return od->entries[i]->hostname;
	} else {
		return NULL;
	}
}

void g2od_set_hostname(OutputDir *od, char *taskID, char *hostname) {
	int		i;

	i = g2od_index_of(od, taskID);
	if (i >= 0) {
		pthread_rwlock_wrlock(&od->rwLock);
		if (od->entries[i]->hostname == NULL) {
			od->entries[i]->hostname = str_dup(hostname);
		}
		pthread_rwlock_unlock(&od->rwLock);
	}
}

off_t g2od_get_file_length(OutputDir *od, char *taskID, int mode) {
	int		i;
	off_t	length = G2OD_LENGTH_NO_SUCH_TASK_ID;

	i = g2od_index_of(od, taskID);
	if (i >= 0) {
		switch (mode) {
			case G2OD_STDOUT: length = od->entries[i]->outLength; break;
			case G2OD_STDERR: length = od->entries[i]->errLength; break;
			default: fatalError("panic", __FILE__, __LINE__);
		}
	}
	srfsLog(LOG_FINE, "g2od_get_file_length %d taskID %s mode %d", length, taskID, mode);
	return length;
}

void g2od_set_file_length(OutputDir *od, char *taskID, off_t length, int mode) {
	int		i;

	srfsLog(LOG_FINE, "g2od_set_file_length %d taskID %s mode %d", length, taskID, mode);
	i = g2od_index_of(od, taskID);
	if (i >= 0) {
		pthread_rwlock_wrlock(&od->rwLock);
		switch (mode) {
			case G2OD_STDOUT: 
				if (od->entries[i]->outLength == G2OD_LENGTH_NOT_SET) {
					od->entries[i]->outLength = length;
				}
				break;
			case G2OD_STDERR:
				if (od->entries[i]->errLength == G2OD_LENGTH_NOT_SET) {
					od->entries[i]->errLength = length;
				}
				break;
			default: fatalError("panic", __FILE__, __LINE__);
		}
		pthread_rwlock_unlock(&od->rwLock);
	} else {
		srfsLog(LOG_WARNING, "Unknown taskID in g2od_set_length %s", taskID);
	}
}
