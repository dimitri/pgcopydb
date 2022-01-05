/*
 * src/bin/pgcopydb/summary.h
 *   Utilities to manage the pgcopydb summary.
 *
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the PostgreSQL License.
 *
 */
#ifndef SUMMARY_H
#define SUMMARY_H

#include <inttypes.h>
#include <signal.h>

#include "postgres_fe.h"
#include "pqexpbuffer.h"
#include "portability/instr_time.h"

#include "string_utils.h"
#include "schema.h"

#define COPY_TABLE_SUMMARY_LINES 8

typedef struct CopyTableSummary
{
	pid_t pid;                  /* pid */
	SourceTable *table;         /* oid, nspname, relname */
	uint64_t startTime;         /* time(NULL) at start time */
	uint64_t doneTime;          /* time(NULL) at done time */
	uint64_t durationMs;        /* instr_time duration in milliseconds */
	instr_time startTimeInstr;  /* internal instr_time tracker */
	instr_time durationInstr;   /* internal instr_time tracker */
	char command[BUFSIZE];      /* SQL command */
} CopyTableSummary;


#define COPY_INDEX_SUMMARY_LINES 8

typedef struct CopyIndexSummary
{
	pid_t pid;                  /* pid */
	SourceIndex *index;         /* oid, nspname, relname */
	uint64_t startTime;         /* time(NULL) at start time */
	uint64_t doneTime;          /* time(NULL) at done time */
	uint64_t durationMs;        /* instr_time duration in milliseconds */
	instr_time startTimeInstr;  /* internal instr_time tracker */
	instr_time durationInstr;   /* internal instr_time tracker */
	char command[BUFSIZE];      /* SQL command */
} CopyIndexSummary;


/*
 * To print the summary, we fill-in a table in-memory and then compute the max
 * size of each column and then we can adjust the display to the actual size
 * needed in there.
 */
typedef struct SummaryTableHeaders
{
	int maxOidSize;
	int maxNspnameSize;
	int maxRelnameSize;
	int maxTableMsSize;
	int maxIndexCountSize;
	int maxIndexMsSize;

	char oidSeparator[NAMEDATALEN];
	char nspnameSeparator[NAMEDATALEN];
	char relnameSeparator[NAMEDATALEN];
	char tableMsSeparator[NAMEDATALEN];
	char indexCountSeparator[NAMEDATALEN];
	char indexMsSeparator[NAMEDATALEN];
} SummaryTableHeaders;

/* Durations are printed as "%2dd%02dh" and the like */
#define INTERVAL_MAXLEN 9

typedef struct SummaryTableEntry
{
	char oid[INTSTRING_MAX_DIGITS];
	char nspname[NAMEDATALEN];
	char relname[NAMEDATALEN];
	char tableMs[INTERVAL_MAXLEN];
	char indexCount[INTSTRING_MAX_DIGITS];
	char indexMs[INTERVAL_MAXLEN];
} SummaryTableEntry;

typedef struct SummaryTable
{
	int count;
	SummaryTableHeaders headers;
	SummaryTableEntry *array;   /* malloc'ed area */
} SummaryTable;


bool write_table_summary(CopyTableSummary *summary, char *filename);
bool read_table_summary(CopyTableSummary *summary, const char *filename);
bool open_table_summary(CopyTableSummary *summary, char *filename);
bool finish_table_summary(CopyTableSummary *summary, char *filename);

bool create_table_index_file(CopyTableSummary *summary,
							 SourceIndexArray *indexArray,
							 char *filename);
bool read_table_index_file(SourceIndexArray *indexArray, char *filename);


bool write_index_summary(CopyIndexSummary *summary, char *filename);
bool read_index_summary(CopyIndexSummary *summary, const char *filename);
bool open_index_summary(CopyIndexSummary *summary, char *filename);
bool finish_index_summary(CopyIndexSummary *summary, char *filename);

bool print_summary(CopyDataSpec *specs);


#endif /* SUMMARY_H */
