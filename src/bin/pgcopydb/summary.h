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

#include "parson.h"

#include "postgres_fe.h"
#include "pqexpbuffer.h"
#include "portability/instr_time.h"

#include "catalog.h"
#include "string_utils.h"
#include "schema.h"

typedef struct CopyTableSummary
{
	pid_t pid;                  /* pid */
	SourceTable *table;         /* oid, nspname, relname */
	uint64_t startTime;         /* time(NULL) at start time */
	uint64_t doneTime;          /* time(NULL) at done time */
	uint64_t durationMs;        /* instr_time duration in milliseconds */
	instr_time startTimeInstr;  /* internal instr_time tracker */
	instr_time durationInstr;   /* internal instr_time tracker */
	uint64_t bytesTransmitted;  /* total number of bytes copied */
	char *command;              /* malloc'ed area */
} CopyTableSummary;


typedef struct CopyVacuumTableSummary
{
	pid_t pid;                  /* pid */
	SourceTable *table;         /* oid, nspname, relname */
	uint64_t startTime;         /* time(NULL) at start time */
	uint64_t doneTime;          /* time(NULL) at done time */
	uint64_t durationMs;        /* instr_time duration in milliseconds */
	instr_time startTimeInstr;  /* internal instr_time tracker */
	instr_time durationInstr;   /* internal instr_time tracker */
} CopyVacuumTableSummary;

typedef struct CopyIndexSummary
{
	pid_t pid;                  /* pid */
	SourceIndex *index;         /* oid, nspname, relname */
	uint64_t startTime;         /* time(NULL) at start time */
	uint64_t doneTime;          /* time(NULL) at done time */
	uint64_t durationMs;        /* instr_time duration in milliseconds */
	instr_time startTimeInstr;  /* internal instr_time tracker */
	instr_time durationInstr;   /* internal instr_time tracker */
	char *command;              /* malloc'ed area */
} CopyIndexSummary;


/* generic data type for OID lookup */
typedef struct CopyOidSummary
{
	pid_t pid;                  /* pid */
	uint64_t startTime;         /* time(NULL) at start time */
	uint64_t doneTime;          /* time(NULL) at done time */
	uint64_t durationMs;        /* instr_time duration in milliseconds */
} CopyOidSummary;

#define COPY_BLOBS_SUMMARY_LINES 3

typedef struct CopyBlobsSummary
{
	pid_t pid;
	uint64_t count;
	uint64_t durationMs;
	uint64_t startTime;
	uint64_t doneTime;
} CopyBlobsSummary;


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
	int maxPartCountSize;
	int maxTableMsSize;
	int maxBytesSize;
	int maxIndexCountSize;
	int maxIndexMsSize;

	char oidSeparator[NAMEDATALEN];
	char nspnameSeparator[NAMEDATALEN];
	char relnameSeparator[NAMEDATALEN];
	char partCountSeparator[NAMEDATALEN];
	char tableMsSeparator[NAMEDATALEN];
	char bytesSeparator[NAMEDATALEN];
	char indexCountSeparator[NAMEDATALEN];
	char indexMsSeparator[NAMEDATALEN];
} SummaryTableHeaders;

/* Durations are printed as "%2dd%02dh" and the like */
#define INTERVAL_MAXLEN 9

typedef struct SummaryIndexEntry
{
	uint32_t oid;
	char oidStr[INTSTRING_MAX_DIGITS];
	char nspname[PG_NAMEDATALEN];
	char relname[PG_NAMEDATALEN];
	char *sql;                  /* malloc'ed area */
	char indexMs[INTERVAL_MAXLEN];
	uint64_t durationMs;
} SummaryIndexEntry;

typedef struct SummaryIndexArray
{
	int count;
	SummaryIndexEntry *array;   /* malloc'ed area */
} SummaryIndexArray;

typedef struct SummaryTableEntry
{
	uint32_t oid;
	char oidStr[INTSTRING_MAX_DIGITS];
	char nspname[PG_NAMEDATALEN];
	char relname[PG_NAMEDATALEN];
	char partCount[INTSTRING_MAX_DIGITS];
	char tableMs[INTERVAL_MAXLEN];
	uint64_t bytes;
	char bytesStr[INTSTRING_MAX_DIGITS];
	char transmitRate[INTSTRING_MAX_DIGITS];
	char indexCount[INTSTRING_MAX_DIGITS];
	char indexMs[INTERVAL_MAXLEN];
	uint64_t durationTableMs;
	uint64_t durationIndexMs;
	SummaryIndexArray indexArray;
	SummaryIndexArray constraintArray;
} SummaryTableEntry;

typedef struct SummaryTable
{
	int count;
	SummaryTableHeaders headers;
	uint64_t totalBytes;
	char totalBytesStr[INTSTRING_MAX_DIGITS];
	SummaryTableEntry *array;   /* calloc'ed area */
} SummaryTable;

/*
 * Keep track of the timing for the main steps of the pgcopydb operations.
 */
extern TopLevelTiming topLevelTimingArray[];

typedef struct Summary
{
	SummaryTable table;
	int tableJobs;
	int indexJobs;
	int vacuumJobs;
	int lObjectJobs;
	int restoreJobs;
} Summary;


bool summary_start_timing(DatabaseCatalog *catalog, TimingSection section);
bool summary_stop_timing(DatabaseCatalog *catalog, TimingSection section);

bool summary_increment_timing(DatabaseCatalog *catalog,
							  TimingSection section,
							  uint64_t count,
							  uint64_t bytes,
							  uint64_t durationMs);

bool summary_set_timing_count(DatabaseCatalog *catalog,
							  TimingSection section,
							  uint64_t count);

bool summary_lookup_timing(DatabaseCatalog *catalog,
						   TopLevelTiming *timing,
						   TimingSection section);

bool summary_pretty_print_timing(DatabaseCatalog *catalog,
								 TopLevelTiming *timing);

/*
 * Summary Iterator
 */
typedef bool (TimingIterFun)(void *context, TopLevelTiming *timing);

typedef struct TimingIterator
{
	DatabaseCatalog *catalog;
	TopLevelTiming *timing;
	SQLiteQuery query;
} TimingIterator;

bool summary_iter_timing(DatabaseCatalog *catalog,
						 void *context,
						 TimingIterFun *callback);

bool summary_iter_timing_init(TimingIterator *iter);
bool summary_iter_timing_next(TimingIterator *iter);
bool catalog_timing_fetch(SQLiteQuery *query);
bool summary_iter_timing_finish(TimingIterator *iter);


/*
 * Internals
 */
bool table_summary_init(CopyTableSummary *summary);
bool table_summary_finish(CopyTableSummary *summary);

bool table_vacuum_summary_init(CopyVacuumTableSummary *summary);
bool table_vacuum_summary_finish(CopyVacuumTableSummary *summary);

bool index_summary_init(CopyIndexSummary *summary);
bool index_summary_finish(CopyIndexSummary *summary);

/*
 * Summary as JSON
 */
bool prepare_table_summary_as_json(CopyTableSummary *summary,
								   JSON_Object *jsobj,
								   const char *key);

bool prepare_index_summary_as_json(CopyIndexSummary *summary,
								   JSON_Object *jsobj,
								   const char *key);

void print_summary_as_json(Summary *summary, const char *filename);

/*
 * Human Readable Summary Table
 */
void print_toplevel_summary(Summary *summary);
void print_summary_table(SummaryTable *summary);
void prepare_summary_table_headers(SummaryTable *summary);

int TopLevelTimingConcurrency(Summary *summary, TopLevelTiming *timing);

#endif /* SUMMARY_H */
