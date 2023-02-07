/*
 * src/bin/pgcopydb/file_utils.h
 *   Utility functions for reading and writing files
 */

#ifndef FILE_UTILS_H
#define FILE_UTILS_H

#include <stdarg.h>

#include "postgres_fe.h"

#include <fcntl.h>


#if defined(__APPLE__)
#define ST_MTIME_S(st) ((int64_t) st.st_mtimespec.tv_sec)
#else
#define ST_MTIME_S(st) ((int64_t) st.st_mtime)
#endif


/*
 * In order to avoid dynamic memory allocations and tracking when searching the
 * PATH environment, we pre-allocate 1024 paths entries. That should be way
 * more than enough for all situations, and only costs 1024*1024 = 1MB of
 * memory.
 */
typedef struct SearchPath
{
	int found;
	char matches[1024][MAXPGPATH];
} SearchPath;

typedef bool (*ReadFromStream) (void *ctx, const char *line, bool *stop);

typedef struct ReadFromStreamContext
{
	int fd;
	uint64_t lineno;
	bool earlyExit;
	ReadFromStream callback;
	void *ctx;                  /* user-defined context */
} ReadFromStreamContext;


bool file_exists(const char *filename);
bool file_is_empty(const char *filename);
bool directory_exists(const char *path);
bool ensure_empty_dir(const char *dirname, int mode);
FILE * fopen_with_umask(const char *filePath, const char *modes, int flags, mode_t umask);
FILE * fopen_read_only(const char *filePath);
bool write_file(char *data, long fileSize, const char *filePath);
bool append_to_file(char *data, long fileSize, const char *filePath);
bool read_file(const char *filePath, char **contents, long *fileSize);
bool read_file_if_exists(const char *filePath, char **contents, long *fileSize);
bool move_file(char *sourcePath, char *destinationPath);
bool duplicate_file(char *sourcePath, char *destinationPath);
bool create_symbolic_link(char *sourcePath, char *targetPath);

bool read_from_stream(FILE *stream, ReadFromStreamContext *context);

void path_in_same_directory(const char *basePath,
							const char *fileName,
							char *destinationPath);

bool search_path_first(const char *filename, char *result, int logLevel);
bool search_path(const char *filename, SearchPath *result);
bool search_path_deduplicate_symlinks(SearchPath *results, SearchPath *dedup);
bool unlink_file(const char *filename);
bool set_program_absolute_path(char *program, int size);
bool normalize_filename(const char *filename, char *dst, int size);

void init_ps_buffer(int argc, char **argv);
void set_ps_title(const char *title);

int fformat(FILE *stream, const char *fmt, ...)
__attribute__((format(printf, 2, 3)));

int sformat(char *str, size_t count, const char *fmt, ...)
__attribute__((format(printf, 3, 4)));

#endif /* FILE_UTILS_H */
