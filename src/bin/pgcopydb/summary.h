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


typedef enum
{
	TIMING_STEP_START = 0,
	TIMING_STEP_BEFORE_SCHEMA_FETCH,
	TIMING_STEP_BEFORE_SCHEMA_DUMP,
	TIMING_STEP_BEFORE_PREPARE_SCHEMA,
	TIMING_STEP_AFTER_PREPARE_SCHEMA,
	TIMING_STEP_BEFORE_FINALIZE_SCHEMA,
	TIMING_STEP_AFTER_FINALIZE_SCHEMA,
	TIMING_STEP_END,
} TimingStep;

typedef struct TopLevelTimings
{
	instr_time startTime;
	instr_time beforeSchemaDump;
	instr_time beforeSchemaFetch;
	instr_time beforePrepareSchema;
	instr_time afterPrepareSchema;
	instr_time beforeFinalizeSchema;
	instr_time afterFinalizeSchema;
	instr_time endTime;

	char totalMs[INTSTRING_MAX_DIGITS];
	char dumpSchemaMs[INTSTRING_MAX_DIGITS];
	char fetchSchemaMs[INTSTRING_MAX_DIGITS];
	char prepareSchemaMs[INTSTRING_MAX_DIGITS];
	char dataAndIndexMs[INTSTRING_MAX_DIGITS];
	char finalizeSchemaMs[INTSTRING_MAX_DIGITS];
	char totalTableMs[INTSTRING_MAX_DIGITS];
	char totalIndexMs[INTSTRING_MAX_DIGITS];
	char blobsMs[INTSTRING_MAX_DIGITS];

	/* allow computing the overhead */
	uint64_t totalDurationMs;   /* wall clock total duration */
	uint64_t schemaDurationMs;  /* dump + prepare + finalize duration */
	uint64_t dumpSchemaDurationMs;
	uint64_t fetchSchemaDurationMs;
	uint64_t prepareSchemaDurationMs;
	uint64_t dataAndIndexesDurationMs;
	uint64_t finalizeSchemaDurationMs;
	uint64_t tableDurationMs;   /* sum of COPY (TABLE DATA) durations */
	uint64_t indexDurationMs;   /* sum of CREATE INDEX durations */
	uint64_t blobDurationMs;

	char totalCopyDataMs[INTSTRING_MAX_DIGITS];
	char totalCreateIndexMs[INTSTRING_MAX_DIGITS];
} TopLevelTimings;


typedef struct Summary
{
	TopLevelTimings timings;
	SummaryTable table;
	int tableJobs;
	int indexJobs;
	int lObjectJobs;
} Summary;


/*
 * Internals
 */
bool table_summary_init(CopyTableSummary *summary);
bool table_summary_finish(CopyTableSummary *summary);

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
 * Large Object top-level like summary
 */
bool write_blobs_summary(CopyBlobsSummary *summary, char *filename);
bool read_blobs_summary(CopyBlobsSummary *summary, char *filename);


/*
 * Top-Level Summary.
 */
void summary_set_current_time(TopLevelTimings *timings, TimingStep step);


/*
 * Human Readable Summary Table
 */
void summary_prepare_toplevel_durations(Summary *summary);
void print_toplevel_summary(Summary *summary);
void print_summary_table(SummaryTable *summary);
void prepare_summary_table_headers(SummaryTable *summary);

#endif /* SUMMARY_H */
