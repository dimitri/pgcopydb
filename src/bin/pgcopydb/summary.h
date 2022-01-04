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


bool write_table_summary(CopyTableSummary *summary, char *filename);
bool read_table_summary(CopyTableSummary *summary, const char *filename);
bool open_table_summary(CopyTableSummary *summary, char *filename);
bool finish_table_summary(CopyTableSummary *summary, char *filename);

bool create_table_index_file(CopyTableSummary *summary,
							 SourceIndexArray *indexArray,
							 char *filename);
bool read_table_index_file(char *filename, SourceIndexArray *indexArray);


bool write_index_summary(CopyIndexSummary *summary, char *filename);
bool read_index_summary(CopyIndexSummary *summary, const char *filename);
bool open_index_summary(CopyIndexSummary *summary, char *filename);
bool finish_index_summary(CopyIndexSummary *summary, char *filename);

#endif /* SUMMARY_H */
