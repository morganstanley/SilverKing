// G2OutputDir.h

#ifndef _G2_OUTPUT_DIR_H_
#define _G2_OUTPUT_DIR_H_

/////////////
// includes

#include <pthread.h>
#include <sys/stat.h>
#include <unistd.h>


////////////
// defines

#define G2OD_LENGTH_NOT_SET -1
#define G2OD_LENGTH_NO_SUCH_TASK_ID -2
#define G2OD_STDOUT 0
#define G2OD_STDERR 1


//////////
// types

typedef struct OutputDirEntry {
	char	*name;
	int		nameSize;
	ino_t	ino;
	off_t	outLength;
	off_t	errLength;
	char	*hostname;
} OutputDirEntry;

typedef struct OutputDir {
	OutputDirEntry	**entries;
	int				numEntries;
	pthread_rwlock_t	rwLock;
} OutputDir;


///////////////
// prototypes

void g2od_delete(OutputDir **od);
OutputDir *g2od_parse(char *def, int length);
void g2od_display(OutputDir *od);

char *g2od_get_hostname(OutputDir *od, char *taskID);
void g2od_set_hostname(OutputDir *od, char *taskID, char *hostname);
off_t g2od_get_file_length(OutputDir *od, char *taskID, int mode);
void g2od_set_file_length(OutputDir *od, char *taskID, off_t length, int mode);


#endif
