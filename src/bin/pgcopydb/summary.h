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


#define COPY_BLOBS_SUMMARY_LINES 3

typedef struct CopyBlobsSummary
{
	pid_t pid;
	uint32_t count;
	uint64_t durationMs;
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


typedef enum
{
	TIMING_STEP_START = 0,
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
	instr_time beforePrepareSchema;
	instr_time afterPrepareSchema;
	instr_time beforeFinalizeSchema;
	instr_time afterFinalizeSchema;
	instr_time endTime;

	char totalMs[INTSTRING_MAX_DIGITS];
	char dumpSchemaMs[INTSTRING_MAX_DIGITS];
	char prepareSchemaMs[INTSTRING_MAX_DIGITS];
	char dataAndIndexMs[INTSTRING_MAX_DIGITS];
	char finalizeSchemaMs[INTSTRING_MAX_DIGITS];
	char totalTableMs[INTSTRING_MAX_DIGITS];
	char totalIndexMs[INTSTRING_MAX_DIGITS];
	char blobsMs[INTSTRING_MAX_DIGITS];

	/* allow computing the overhead */
	uint64_t totalDurationMs;   /* wall clock total duration */
	uint64_t schemaDurationMs;  /* dump + prepare + finalize duration */
	uint64_t dataAndIndexesDurationMs;
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
} Summary;


bool write_table_summary(CopyTableSummary *summary, char *filename);
bool read_table_summary(CopyTableSummary *summary, const char *filename);
bool open_table_summary(CopyTableSummary *summary, char *filename);
bool finish_table_summary(CopyTableSummary *summary, char *filename);

bool prepare_table_summary_as_json(CopyTableSummary *summary,
								   JSON_Object *jsobj,
								   const char *key);

bool create_table_index_file(CopyTableSummary *summary,
							 SourceIndexArray *indexArray,
							 char *filename);
bool read_table_index_file(SourceIndexArray *indexArray, char *filename);

bool write_blobs_summary(CopyBlobsSummary *summary, char *filename);
bool read_blobs_summary(CopyBlobsSummary *summary, char *filename);


void summary_set_current_time(TopLevelTimings *timings, TimingStep step);


bool write_index_summary(CopyIndexSummary *summary, char *filename,
						 bool constraint);
bool read_index_summary(CopyIndexSummary *summary, const char *filename);
bool open_index_summary(CopyIndexSummary *summary, char *filename,
						bool constraint);
bool finish_index_summary(CopyIndexSummary *summary, char *filename,
						  bool constraint);

bool prepare_index_summary_as_json(CopyIndexSummary *summary,
								   JSON_Object *jsobj,
								   const char *key);

void summary_prepare_toplevel_durations(Summary *summary);
void print_toplevel_summary(Summary *summary, int tableJobs, int indexJobs);
void print_summary_table(SummaryTable *summary);
void prepare_summary_table_headers(SummaryTable *summary);

#endif /* SUMMARY_H */
