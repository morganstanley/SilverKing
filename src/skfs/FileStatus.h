// FileStatus.h

#ifndef _FILE_STATUS_H_
#define _FILE_STATUS_H_

/////////////
// includes

#include <stdint.h>


//////////
// types

typedef uint16_t FileStatus;


///////////////
// prototypes

int fs_get_deleted(FileStatus *fs);
void fs_set_deleted(FileStatus *fs, int deleted);
const char *fs_to_string(FileStatus *fs);
#endif