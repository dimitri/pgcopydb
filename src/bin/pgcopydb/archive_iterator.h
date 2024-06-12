/*
 * src/bin/pgcopydb/archive_iterator.h
 *   Implementations of a file iterator for reading new lıne seperated files
 */

#ifndef ARCHIVE_ITER_H
#define ARCHIVE_ITER_H

#include <stdbool.h>

typedef struct ArchiveIterator ArchiveIterator;
typedef struct ArchiveContentItem ArchiveContentItem;

ArchiveIterator * archive_iterator_from(const char *filename);

bool archive_iterator_next(ArchiveIterator *iterator, ArchiveContentItem **item);

void archive_iterator_destroy(ArchiveIterator *iterator);

#endif /* ARCHIVE_ITER_H */
