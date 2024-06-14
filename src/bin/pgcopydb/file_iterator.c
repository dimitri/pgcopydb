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
} FileIterator;

/*
 * Create a new iterator from the file name
 */
static FileIterator *
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
static bool
file_iterator_next(FileIterator *iterator, char **line)
{
	*line = NULL;
	if (fgets(iterator->line, sizeof(iterator->line), iterator->file) != NULL)
	{
		/* replace the new line character with null terminator */
		iterator->line[strcspn(iterator->line, "\n")] = '\0';
		*line = iterator->line;
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
 * Destroy the iterator and free resources
 */
static bool
file_iterator_destroy(FileIterator *iterator)
{
	if (iterator->file)
	{
		fclose(iterator->file);
	}

	return true;
}


bool
file_iter(const char *filename,
		  void *context,
		  FileIterCallback *callback)
{
	FileIterator *iter = file_iterator_from(filename);
	if (!iter)
	{
		/* errors have already been logged */
		return false;
	}

	char *line = NULL;
	for (;;)
	{
		if (!file_iterator_next(iter, &line))
		{
			/* errors have already been logged */
			return false;
		}

		if (line == NULL)
		{
			if (!file_iterator_destroy(iter))
			{
				/* errors have already been logged */
				return false;
			}

			break;
		}

		/* now call the provided callback */
		if (!(*callback)(context, line))
		{
			log_error("Failed to iterate over list of colls, "
					  "see above for details");
			return false;
		}
	}


	return true;
}
