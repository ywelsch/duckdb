// Measures raw fsync / F_FULLFSYNC latency on this machine: appends a small
// write and syncs, in a loop, reporting average latency.
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

int main(int argc, char **argv) {
	if (argc != 3) {
		fprintf(stderr, "Usage: %s <path> <fsync|fullfsync>\n", argv[0]);
		return 1;
	}
	int use_full = strcmp(argv[2], "fullfsync") == 0;
	int fd = open(argv[1], O_CREAT | O_WRONLY | O_TRUNC, 0644);
	if (fd < 0) {
		perror("open");
		return 1;
	}
	const int iters = 200;
	char buf[128];
	memset(buf, 'x', sizeof(buf));
	struct timespec start, end;
	clock_gettime(CLOCK_MONOTONIC, &start);
	for (int i = 0; i < iters; i++) {
		if (write(fd, buf, sizeof(buf)) != sizeof(buf)) {
			perror("write");
			return 1;
		}
		if (use_full) {
			if (fcntl(fd, F_FULLFSYNC) != 0) {
				perror("F_FULLFSYNC");
				return 1;
			}
		} else {
			if (fsync(fd) != 0) {
				perror("fsync");
				return 1;
			}
		}
	}
	clock_gettime(CLOCK_MONOTONIC, &end);
	double secs = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;
	printf("%s: %d iters in %.3fs -> %.3f ms/op (%.1f ops/s)\n", use_full ? "F_FULLFSYNC" : "fsync", iters, secs,
	       secs * 1000.0 / iters, iters / secs);
	close(fd);
	return 0;
}
