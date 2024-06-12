/*
 * src/bin/pgcopydb/file_iterator.h
 *   Implementations of a file iterator for reading new lıne seperated files
 */

#ifndef FILE_ITER_H
#define FILE_ITER_H

#include <stdlib.h>
#include <stdbool.h>

typedef struct FileIterator FileIterator;

FileIterator * file_iterator_from(const char *filename);

bool file_iterator_next(FileIterator *iterator, char **line);

size_t file_iterator_get_line_number(FileIterator *iterator);

const char * file_iterator_get_file_name(FileIterator *iterator);

void file_iterator_destroy(FileIterator *iterator);

#endif /* FILE_ITER_H */
