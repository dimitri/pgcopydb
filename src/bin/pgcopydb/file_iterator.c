/*
 * src/bin/pgcopydb/file_iterator.c
 *   Implementations of a file iterator for reading new lıne seperated files
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "defaults.h"
#include "log.h"
#include "file_iterator.h"
#include "file_utils.h"

/*
 * A struct to hold the state of the file iterator. This
 * iterator is used to iterate a file line-by-line.
 */
typedef struct FileIterator
{
	FILE *file;
	const char *filename;
	char line[BUFSIZE];
	size_t line_num;
} FileIterator;

/*
 * Create a new iterator from the file name
 */
FileIterator *
file_iterator_from(const char *filename)
{
	FileIterator *iterator = (FileIterator *) calloc(1, sizeof(FileIterator));
	if (iterator == NULL)
	{
		log_error("Failed to allocate memory for FileIterator");
		return NULL;
	}

	iterator->filename = filename;
	iterator->file = fopen_read_only(filename);
	if (iterator->file == NULL)
	{
		log_error("Failed to open file");
		return NULL;
	}

	return iterator;
}


/*
 * Get the next line/item from the file/iterator.
 *
 * The memory for the line is allocated by this function and will be freed by it too.
 */
bool
file_iterator_next(FileIterator *iterator, char **line)
{
	*line = NULL;
	iterator->line_num = 0;
	if (fgets(iterator->line, sizeof(iterator->line), iterator->file) != NULL)
	{
		/* replace the new line character with null terminator */
		iterator->line[strcspn(iterator->line, "\n")] = '\0';
		*line = iterator->line;
		iterator->line_num++;
		return true;
	}

	if (ferror(iterator->file))
	{
		log_error("Failed to read line from file %s", iterator->filename);
		return false;
	}

	return true;
}


/*
 * Get the line number
 */
size_t
file_iterator_get_line_number(FileIterator *iterator)
{
	return iterator->line_num;
}


/*
 * Get the file name
 */
const char *
file_iterator_get_file_name(FileIterator *iterator)
{
	return iterator->filename;
}


/*
 * Destroy the iterator and free resources
 */
void
file_iterator_destroy(FileIterator *iterator)
{
	if (iterator->file)
	{
		fclose(iterator->file);
	}
}
