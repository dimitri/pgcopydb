/*
 * src/bin/pgcopydb/summary.c
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>

#include "copydb.h"
#include "env_utils.h"
#include "log.h"
#include "pidfile.h"
#include "schema.h"
#include "signals.h"
#include "string_utils.h"
#include "summary.h"


/*
 * create_table_summary creates a summary file for the copy operation of a
 * given table. The summary file contains identification information and
 * duration information and can be used both as a lock file and as a resource
 * file to display what's happening.
 */
bool
write_table_summary(CopyTableSummary *summary, char *filename)
{
	char contents[BUFSIZE] = { 0 };

	sformat(contents, BUFSIZE,
			"%d\n%u\n%s\n%s\n%lld\n%lld\n%lld\n%s\n",
			summary->pid,
			summary->table->oid,
			summary->table->nspname,
			summary->table->relname,
			(long long) summary->startTime,
			(long long) summary->doneTime,
			(long long) summary->durationMs,
			summary->command);

	/* write the summary to the doneFile */
	return write_file(contents, strlen(contents), filename);
}


/*
 * read_table_summary reads a summary on-disk and parses the content of the
 * file, and populates the given summary with what we read in the on-disk file.
 *
 * Couple notes:
 *
 *  1. the summary->table SourceTable pointer should point to allocated memory,
 *
 *  2. the instr_time fields can't be read from their on-disk representation
 *     and are set to zero instead.
 */
bool
read_table_summary(CopyTableSummary *summary, const char *filename)
{
	char *fileContents = NULL;
	long fileSize = 0L;

	if (!read_file(filename, &fileContents, &fileSize))
	{
		/* errors have already been logged */
		return false;
	}

	char *fileLines[BUFSIZE] = { 0 };
	int lineCount = splitLines(fileContents, fileLines, BUFSIZE);

	if (lineCount < COPY_TABLE_SUMMARY_LINES)
	{
		log_error("Failed to parse summary file \"%s\" which contains only "
				  "%d lines, at least %d lines are expected",
				  filename,
				  lineCount,
				  COPY_TABLE_SUMMARY_LINES);

		free(fileContents);

		return false;
	}

	if (!stringToInt(fileLines[0], &(summary->pid)))
	{
		/* errors have already been logged */
		return false;
	}

	/* better not point to NULL */
	SourceTable *table = summary->table;

	if (table == NULL)
	{
		log_error("BUG: read_table_summary summary->table is NULL");
		return false;
	}

	if (!stringToUInt32(fileLines[1], &(table->oid)))
	{
		/* errors have already been logged */
		return false;
	}

	strlcpy(table->nspname, fileLines[2], sizeof(table->nspname));
	strlcpy(table->relname, fileLines[3], sizeof(table->relname));

	if (!stringToUInt64(fileLines[4], &(summary->startTime)))
	{
		/* errors have already been logged */
		return false;
	}

	if (!stringToUInt64(fileLines[5], &(summary->doneTime)))
	{
		/* errors have already been logged */
		return false;
	}

	if (!stringToUInt64(fileLines[6], &(summary->durationMs)))
	{
		/* errors have already been logged */
		return false;
	}

	/* last summary line in the file is the SQL command */
	strlcpy(summary->command, fileLines[7], sizeof(summary->command));

	/* we can't provide instr_time readers */
	summary->startTimeInstr = (instr_time) {
		0
	};
	summary->durationInstr = (instr_time) {
		0
	};

	return true;
}


/*
 * open_table_summary initializes the time elements of a table summary and
 * writes the summary in the given filename. Typically, the lockFile.
 */
bool
open_table_summary(CopyTableSummary *summary, char *filename)
{
	summary->startTime = time(NULL);
	summary->doneTime = 0;
	summary->durationMs = 0;
	summary->startTimeInstr = (instr_time) {
		0
	};
	summary->durationInstr = (instr_time) {
		0
	};

	INSTR_TIME_SET_CURRENT(summary->startTimeInstr);

	return write_table_summary(summary, filename);
}


/*
 * finish_table_summary sets the duration of the summary fields and writes the
 * summary in the given filename. Typically, the doneFile.
 */
bool
finish_table_summary(CopyTableSummary *summary, char *filename)
{
	summary->doneTime = time(NULL);

	INSTR_TIME_SET_CURRENT(summary->durationInstr);
	INSTR_TIME_SUBTRACT(summary->durationInstr, summary->startTimeInstr);

	summary->durationMs = INSTR_TIME_GET_MILLISEC(summary->durationInstr);

	return write_table_summary(summary, filename);
}


/*
 * create_table_index_file creates a file with one line per index attached to a
 * table. Each line contains only the index oid, from which we can find the
 * index doneFile.
 */
bool
create_table_index_file(CopyTableSummary *summary,
						SourceIndexArray *indexArray,
						char *filename)
{
	if (indexArray->count < 1)
	{
		return true;
	}

	PQExpBuffer content = createPQExpBuffer();

	if (content == NULL)
	{
		log_fatal("Failed to allocate memory to create the "
				  " index list file \"%s\"", filename);
		return false;
	}

	for (int i = 0; i < indexArray->count; i++)
	{
		SourceIndex *index = &(indexArray->array[i]);

		appendPQExpBuffer(content, "%d\n", index->indexOid);
	}

	/* memory allocation could have failed while building string */
	if (PQExpBufferBroken(content))
	{
		log_error("Failed to create file \"%s\": out of memory", filename);
		destroyPQExpBuffer(content);
		return false;
	}

	bool success = write_file(content->data, content->len, filename);
	destroyPQExpBuffer(content);

	return success;
}


/*
 * read_table_index_file reads an index list file and populates an array of
 * indexes with only the indexOid information. The actual array memory
 * allocation is done in the function.
 */
bool
read_table_index_file(char *filename, SourceIndexArray *indexArray)
{
	char *fileContents = NULL;
	long fileSize = 0L;

	if (!read_file(filename, &fileContents, &fileSize))
	{
		/* errors have already been logged */
		return false;
	}

	char *fileLines[BUFSIZE] = { 0 };
	int lineCount = splitLines(fileContents, fileLines, BUFSIZE);

	/* we expect to have an indexOid per line, no comments, etc */
	indexArray->count = lineCount;
	indexArray->array = (SourceIndex *) malloc(lineCount * sizeof(SourceIndex));

	for (int i = 0; i < lineCount; i++)
	{
		SourceIndex *index = &(indexArray->array[i]);

		if (!stringToUInt32(fileLines[i], &(index->indexOid)))
		{
			log_error("Failed to read the index oid \"%s\" "
					  "in file \"%s\" at line %d",
					  fileLines[i],
					  filename,
					  i);
			return false;
		}
	}

	return true;
}


/*
 * write_index_summary writes the current Index Summary to given filename.
 */
bool
write_index_summary(CopyIndexSummary *summary, char *filename)
{
	char contents[BUFSIZE] = { 0 };

	sformat(contents, BUFSIZE,
			"%d\n%u\n%s\n%s\n%lld\n%lld\n%lld\n%s\n",
			summary->pid,
			summary->index->indexOid,
			summary->index->indexNamespace,
			summary->index->indexRelname,
			(long long) summary->startTime,
			(long long) summary->doneTime,
			(long long) summary->durationMs,
			summary->command);

	/* write the summary to the doneFile */
	return write_file(contents, strlen(contents), filename);
}


/*
 * read_index_summary reads back in-memory a summary from disk.
 */
bool
read_index_summary(CopyIndexSummary *summary, const char *filename)
{
	char *fileContents = NULL;
	long fileSize = 0L;

	if (!read_file(filename, &fileContents, &fileSize))
	{
		/* errors have already been logged */
		return false;
	}

	char *fileLines[BUFSIZE] = { 0 };
	int lineCount = splitLines(fileContents, fileLines, BUFSIZE);

	if (lineCount < COPY_TABLE_SUMMARY_LINES)
	{
		log_error("Failed to parse summary file \"%s\" which contains only "
				  "%d lines, at least %d lines are expected",
				  filename,
				  lineCount,
				  COPY_TABLE_SUMMARY_LINES);

		free(fileContents);

		return false;
	}

	if (!stringToInt(fileLines[0], &(summary->pid)))
	{
		/* errors have already been logged */
		return false;
	}

	/* better not point to NULL */
	SourceIndex *index = summary->index;

	if (index == NULL)
	{
		log_error("BUG: read_index_summary summary->index is NULL");
		return false;
	}

	if (!stringToUInt32(fileLines[1], &(index->indexOid)))
	{
		/* errors have already been logged */
		return false;
	}

	strlcpy(index->indexNamespace, fileLines[2], sizeof(index->indexNamespace));
	strlcpy(index->indexRelname, fileLines[3], sizeof(index->indexRelname));

	if (!stringToUInt64(fileLines[4], &(summary->startTime)))
	{
		/* errors have already been logged */
		return false;
	}

	if (!stringToUInt64(fileLines[5], &(summary->doneTime)))
	{
		/* errors have already been logged */
		return false;
	}

	if (!stringToUInt64(fileLines[6], &(summary->durationMs)))
	{
		/* errors have already been logged */
		return false;
	}

	/* last summary line in the file is the SQL command */
	strlcpy(summary->command, fileLines[7], sizeof(summary->command));

	/* we can't provide instr_time readers */
	summary->startTimeInstr = (instr_time) {
		0
	};
	summary->durationInstr = (instr_time) {
		0
	};

	return true;
}


/*
 * open_index_summary initializes the time elements of an index summary and
 * writes the summary in the given filename. Typically, the lockFile.
 */
bool
open_index_summary(CopyIndexSummary *summary, char *filename)
{
	summary->startTime = time(NULL);
	summary->doneTime = 0;
	summary->durationMs = 0;
	summary->startTimeInstr = (instr_time) {
		0
	};
	summary->durationInstr = (instr_time) {
		0
	};

	INSTR_TIME_SET_CURRENT(summary->startTimeInstr);

	return write_index_summary(summary, filename);
}


/*
 * finish_index_summary sets the duration of the summary fields and writes the
 * summary in the given filename. Typically, the doneFile.
 */
bool
finish_index_summary(CopyIndexSummary *summary, char *filename)
{
	summary->doneTime = time(NULL);

	INSTR_TIME_SET_CURRENT(summary->durationInstr);
	INSTR_TIME_SUBTRACT(summary->durationInstr, summary->startTimeInstr);

	summary->durationMs = INSTR_TIME_GET_MILLISEC(summary->durationInstr);

	return write_index_summary(summary, filename);
}
