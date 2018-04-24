// PartialBlockReader.c

/////////////
// includes

#include "FileBlockID.h"
#include "PartialBlockReader.h"
#include "PartialBlockReadRequest.h"
#include "Util.h"

#include <errno.h>
#include <stdint.h>


////////////
// defines

#define _PBR_MAX_NATIVE_READ_ATTEMPTS 8
#define _PBR_NATIVE_READ_ERROR_SLEEP_MICROS 200
#define _PBR_MAX_SKFS_BLOCK_READ_RETRIES 8
#define _PBR_SKFS_READ_ERROR_SLEEP_MICROS (200 * 1000)


///////////////////
// implementation

PartialBlockReader *pbr_new(AttrReader *ar, FileBlockReader *fbr, G2TaskOutputReader *g2tor) {
	PartialBlockReader *pbr;

	pbr = (PartialBlockReader*)mem_alloc(1, sizeof(PartialBlockReader));
	pbr->ar = ar;
	pbr->fbr = fbr;
	pbr->g2tor = g2tor;
	return pbr;
}

void pbr_delete(PartialBlockReader **pbr) {
	if (pbr != NULL && *pbr != NULL) {
		mem_free((void **)pbr);
	} else {
		fatalError("bad ptr in pbr_delete");
	}
}

int pbr_read(PartialBlockReader *pbr, const char *path, char *dest, size_t readSize, off_t readOffset, SKFSOpenFile *sof) {
    FileAttr	fa;
	FileAttr	*_fa;
    
	srfsLog(LOG_FINE, "pbr_read %s %d %d", path, readSize, readOffset);
	if (readSize == 0) {
		srfsLog(LOG_WARNING, "readFromFile ignoring zero-byte read");
		return 0;
	}
    
    if (sof != NULL && sof->attr != NULL) {
        _fa = sof->attr;
    } else {
        int			result;
        int         attrFoundInDHT;
        
        _fa = &fa;
        memset(&fa, 0, sizeof(FileAttr));
        result = ar_get_attr(pbr->ar, (char *)path, &fa);
        if (result != 0) {
            if (!is_writable_path(path)) {
                srfsLog(LOG_WARNING, "Error reading %s result %d", path, result);
                fatalError("readFromFile() failed", __FILE__, __LINE__);
            } else {
                return -ENOENT;
            }
        }
    }
    return pbr_read_given_attr(pbr, path, dest, readSize, readOffset, _fa, TRUE);
}
    
static int _pbr_native_read(PartialBlockReader *pbr, const char *path, char *dest, size_t readSize, off_t readOffset) {
    int fd;
    int attemptIndex;
    size_t  totalRead;
    ssize_t numRead;
    
    totalRead = 0;
    attemptIndex = 0;
    while (totalRead < readSize) {    
        char nfsPath[SRFS_MAX_PATH_LENGTH];

        ar_translate_path(pbr->ar, nfsPath, path);
        fd = open(nfsPath, O_RDONLY | O_NOFOLLOW);
        if (fd != -1) {
            int		closeResult;
            
            numRead = pread(fd, dest + totalRead, readSize - totalRead, 
                            readOffset + totalRead);
            if (numRead < 0) {
                srfsLog(LOG_WARNING, "_pbr_native_read read() error %d %s %d", 
                                    errno, nfsPath, attemptIndex);
            }
            closeResult = close(fd);
            if (closeResult != 0) {
                srfsLog(LOG_WARNING, "Ignoring failed close %s %d", nfsPath, errno);
            }
        } else {
            numRead = -1;
        }
    
        if (numRead < 0) {
            srfsLog(LOG_WARNING, "_pbr_native_read error %d %s %d", errno, nfsPath, attemptIndex);
            if (errno == EPERM || errno == ENOENT) {
                return -1;
            }
            ++attemptIndex;
            if (attemptIndex > _PBR_MAX_NATIVE_READ_ATTEMPTS) {
                return -1;
            } else {
                usleep(_PBR_NATIVE_READ_ERROR_SLEEP_MICROS);
            }
        } else {
            attemptIndex = 0;
            totalRead += numRead;
        }
    }
    return totalRead;
}
    
int pbr_read_given_attr(PartialBlockReader *pbr, const char *path, char *dest, size_t readSize, off_t readOffset, FileAttr *fa, int presumeBlocksInDHT, int maxBlocksReadAhead, int useNFSReadAhead) {    
	int			numBlocks;
	uint64_t	firstBlock;
	uint64_t	lastBlock;
    uint64_t    readAheadFirstBlock;
	off_t		readEnd;
	off_t		actualReadSize;
	off_t		totalRead;
	off_t		totalSize;
	int			i;
	int			numBlocksReadAhead;
    
	if (readOffset >= fa->stat.st_size) {
		if (readOffset == fa->stat.st_size) {
			srfsLog(LOG_FINE, "returning EOF");
			return 0;
		} else {
			srfsLog(LOG_WARNING, "Error requested readOffset %ld is past end of file %ld", readOffset, fa->stat.st_size);
			return -1;
		}
	}
    
    srfsLog(LOG_FINE, "pbr_read_given_attr %s %d %d presumeBlocksInDHT %d", path, readSize, readOffset, presumeBlocksInDHT);

    // FUTURE: probably remove g2tor
	//if (g2tor_is_g2tor_path(pbr->g2tor, path)) {
	//	return g2tor_read(pbr->g2tor, path, dest, readSize, readOffset);
	//}

	readEnd = off_min(readOffset + readSize, fa->stat.st_size);
	srfsLog(LOG_FINE, "readEnd %d readOffset %ld", readEnd, readOffset);
	actualReadSize = readEnd - readOffset;
	srfsLog(LOG_FINE, "actualReadSize %d", actualReadSize);

	firstBlock = offsetToBlock(readOffset);
	lastBlock = offsetToBlock(readOffset + actualReadSize - 1);
	numBlocks = lastBlock - firstBlock + 1;
	srfsLog(LOG_FINE, "firstBlock %d lastBlock %d numBlocks %d", firstBlock, lastBlock, numBlocks);

	// compute readahead
    if (maxBlocksReadAhead > 0) {
        if (actualReadSize >= PBR_READAHEAD_THRESHOLD) {
            if (lastBlock < offsetToBlock(fa->stat.st_size - 1)) {
                numBlocksReadAhead = int_min(offsetToBlock(fa->stat.st_size - 1) - lastBlock, PBR_MAX_READAHEAD_BLOCKS);
            } else {
                numBlocksReadAhead = 0;
            }
        } else {
            numBlocksReadAhead = 0;
        }
        numBlocksReadAhead = int_min(numBlocksReadAhead, maxBlocksReadAhead);
    } else {
        numBlocksReadAhead = 0;
    }
	srfsLog(LOG_FINE, "numBlocksReadAhead %d", numBlocksReadAhead);

    // dest == NULL ==> purely read-ahead, convert the request
    if (dest == NULL) {
        numBlocksReadAhead += numBlocks;
        numBlocks = 0;
        readAheadFirstBlock = firstBlock;
    } else {
        readAheadFirstBlock = lastBlock + 1;
    }

	{
		PartialBlockReadRequest	*pbrrs[numBlocks];
		FileBlockID	*fbids[numBlocks];
		FileBlockID	*fbidsReadAhead[numBlocksReadAhead];
		PartialBlockReadRequest *pbrrsReadAhead[numBlocksReadAhead];

		for (i = 0; i < numBlocks; i++) {
			fbids[i] = fbid_new(&fa->fid, firstBlock + i);
		}
		totalSize = 0;
		for (i = 0; i < numBlocks; i++) {
			size_t	blockReadOffset;
			size_t	blockReadSize;
			size_t	blockReadEnd;

			if (i == 0) {
				blockReadOffset = readOffset % SRFS_BLOCK_SIZE;
				blockReadEnd = size_min(blockReadOffset + actualReadSize, SRFS_BLOCK_SIZE);
			} else if (i == numBlocks - 1) {
				blockReadOffset = 0;
				if (actualReadSize == 0) {
					blockReadEnd = 0;
				} else {
					blockReadEnd = (readOffset + actualReadSize) % SRFS_BLOCK_SIZE;
					if (blockReadEnd == 0) {
						blockReadEnd = SRFS_BLOCK_SIZE;
					}
				}
			} else {
				blockReadOffset = 0;
				blockReadEnd = SRFS_BLOCK_SIZE;
			}
			blockReadSize = blockReadEnd - blockReadOffset;
			pbrrs[i] = pbrr_new(fbids[i], dest + totalSize, blockReadOffset, blockReadSize, 
                                stat_mtime_micros(&fa->stat));
			totalSize += blockReadSize;
		}
		if (dest != NULL && totalSize != actualReadSize) {
			srfsLog(LOG_WARNING, "totalSize %d actualReadSize %d", totalSize, actualReadSize);
			fatalError("totalSize != actualReadSize", __FILE__, __LINE__);
		}

		for (i = 0; i < numBlocksReadAhead; i++) {
			fbidsReadAhead[i] = fbid_new(&fa->fid, readAheadFirstBlock + i);
			pbrrsReadAhead[i] = pbrr_new(fbidsReadAhead[i], NULL, 0, 0, 
                                    stat_mtime_micros(&fa->stat));
		}

		totalRead = fbr_read(pbr->fbr, pbrrs, numBlocks, pbrrsReadAhead, numBlocksReadAhead, presumeBlocksInDHT, useNFSReadAhead);
		if (dest != NULL && totalRead != actualReadSize) {
			srfsLog(LOG_WARNING, "totalRead %d actualReadSize %d", totalRead, actualReadSize);
			if (totalRead != -1) {
				//fatalError("totalRead != actualReadSize", __FILE__, __LINE__);
				srfsLog(LOG_WARNING, "totalRead != actualReadSize");
			} else {
				srfsLog(LOG_WARNING, "pbr_read received error from fbr_read");
                if (!is_writable_path(path)) {
                    totalRead = _pbr_native_read(pbr, path, dest, readSize, readOffset);
                    if (totalRead <= 0) {
                        totalRead = -1;
                        srfsLog(LOG_WARNING, "pbr_read returning error after _pbr_native_read");
                    }
                } else {
                    int ii;
                    
                    for (ii = 0; totalRead != actualReadSize && ii < _PBR_MAX_SKFS_BLOCK_READ_RETRIES; ii++) {
                        totalRead = fbr_read(pbr->fbr, pbrrs, numBlocks, pbrrsReadAhead, numBlocksReadAhead, presumeBlocksInDHT, useNFSReadAhead);
                        srfsLog(LOG_WARNING, "skfs block read retry %d %d", totalRead, actualReadSize);
                        if (totalRead != actualReadSize) {
                            usleep(_PBR_SKFS_READ_ERROR_SLEEP_MICROS);
                        }
                    }
                    if (totalRead != actualReadSize) {
                        srfsLog(LOG_WARNING, "writable path. pbr_read returning error");
                        totalRead = -1;
                    }
                }
			}
		}

		srfsLog(LOG_FINE, "freeing file read resources");
		for (i = 0; i < numBlocks; i++) {
			fbid_delete(&fbids[i]);
			pbrr_delete(&pbrrs[i]);
		}

		for (i = 0; i < numBlocksReadAhead; i++) {
			fbid_delete(&fbidsReadAhead[i]);
			pbrr_delete(&pbrrsReadAhead[i]);
		}
	}

#if 0
	srfsLog(LOG_WARNING, "\nFile read stats");
	ac_display_stats(pbr->ar->attrCache);
	fbc_display_stats(pbr->fbr->fileBlockCache);
#endif

	srfsLog(LOG_FINE, "readFromFile end %s %d", path, totalRead);
	return totalRead;
}
