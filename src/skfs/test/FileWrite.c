#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


#define BLOCK_SIZE 131072

static char buf[BLOCK_SIZE];

int main(int argc, char **argv) {
	if (argc != 3) {
		printf("args: <file> <size>");
		return -1;
	} else {
		char	*file;
		size_t	size;
		int	fd;
		size_t	totalWritten;
		int	rc;

		file = argv[1];
		size = atol(argv[2]);
		fd = open(file, O_WRONLY | O_CREAT | O_TRUNC, 00777);
		if (fd == -1) {
			printf("open error\n");
			return -1;
		}
		totalWritten = 0;
		while (totalWritten < size) {
			ssize_t	opWritten;
			size_t	numToWrite;

			numToWrite = size - totalWritten;
			if (numToWrite > BLOCK_SIZE) {
				numToWrite = BLOCK_SIZE;
			}
			opWritten = write(fd, buf, numToWrite);
			if (opWritten < 0) {
				printf("Error at %d\n", totalWritten);
				return -1;
			} else {
				totalWritten += opWritten;
			}
		}	
		rc = close(fd);
		if (rc == -1) {
			printf("close error\n");
			return -1;
		}
		return 0;
	}
}
