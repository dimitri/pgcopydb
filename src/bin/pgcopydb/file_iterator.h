/*
 * src/bin/pgcopydb/file_iterator.h
 *   Implementations of a file iterator for reading new lıne seperated files
 */

#ifndef FILE_ITER_H
#define FILE_ITER_H

#include <stdbool.h>

typedef bool (FileIterCallback)(void *context, const char *item);

/*
 * Iterate over the file line-by-line and call the callback function for each line.
 */
bool file_iter(const char *filename,
			   void *context,
			   FileIterCallback *callback);

#endif /* FILE_ITER_H */
