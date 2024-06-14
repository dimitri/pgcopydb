/*
 * src/bin/pgcopydb/archive_iterator.h
 *   Implementations of a file iterator for reading new lıne seperated files
 */

#ifndef ARCHIVE_ITER_H
#define ARCHIVE_ITER_H

#include <stdbool.h>

typedef struct ArchiveContentItem ArchiveContentItem;

typedef bool (ArchiveIterCallback)(void *context, ArchiveContentItem *item);

/*
 * Iterate over the archive and call the callback function for each archive item.
 */
bool archive_iter(const char *filename,
				  void *context,
				  ArchiveIterCallback *callback);

#endif /* ARCHIVE_ITER_H */
