/*
 * src/bin/pgcopydb/archive_iterator.h
 *   Implementations of a file iterator for reading new lıne seperated files
 */

#ifndef ARCHIVE_ITER_H
#define ARCHIVE_ITER_H

#include <stdbool.h>

typedef struct ArchiveContentItem ArchiveContentItem;

typedef bool (ArchiveterFun)(void *context, ArchiveContentItem *item);

bool archive_iter(const char *filename,
				  void *context,
				  ArchiveterFun *callback);

#endif /* ARCHIVE_ITER_H */
