/*
 * src/bin/pgcopydb/cli_common.h
 *     Implementation of a CLI which lets you run individual keeper routines
 *     directly
 *
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the PostgreSQL License.
 *
 */

#ifndef CLI_COMMON_H
#define CLI_COMMON_H

#include <getopt.h>
#include <stdbool.h>

#include "copydb_paths.h"
#include "defaults.h"
#include "parson.h"
#include "parsing_utils.h"
#include "pgcmd.h"
#include "pgsql.h"

typedef struct SplitTableLargerThan
{
	uint64_t bytes;
	char bytesPretty[NAMEDATALEN];
} SplitTableLargerThan;


typedef struct SentinelOptions
{
	/* pgcopydb stream sentinel get --flush-lsn */
	bool startpos;
	bool endpos;
	bool apply;
	bool writeLSN;
	bool transformLSN;
	bool flushLSN;
	bool replayLSN;

	/* pgcopydb stream sentinel set endpos --current */
	bool currentLSN;
} SentinelOptions;


typedef struct CopyDBOptions
{
	char dir[MAXPGPATH];

	ConnStrings connStrings;

	int tableJobs;
	int indexJobs;
	int lObjectJobs;

	SplitTableLargerThan splitTablesLargerThan;
	int splitMaxParts;
	bool estimateTableSizes;

	RestoreOptions restoreOptions;

	bool roles;
	bool skipLargeObjects;
	bool skipExtensions;
	bool skipCommentOnExtension;
	bool skipCollations;
	bool skipVacuum;
	bool skipAnalyze;
	bool skipDBproperties;
	bool skipCtidSplit;
	bool noRolesPasswords;
	bool failFast;
	bool useCopyBinary;

	bool restart;
	bool resume;
	bool notConsistent;

	ReplicationSlot slot;
	char snapshot[BUFSIZE];
	char origin[BUFSIZE];

	bool stdIn;
	bool stdOut;

	bool follow;
	bool createSlot;

	/* pgcopydb stream sentinel get --flush-lsn and friends */
	SentinelOptions sentinelOptions;

	/* pgcopydb stream receive|transform|apply --endpos %X%X */
	uint64_t endpos;

	char filterFileName[MAXPGPATH];
	char requirementsFileName[MAXPGPATH];
} CopyDBOptions;

extern bool outputJSON;
extern CopyDBOptions copyDBoptions;

void cli_help(int argc, char **argv);

int cli_print_version_getopts(int argc, char **argv);
void cli_print_version(int argc, char **argv);

void cli_pprint_json(JSON_Value *js);
char * logLevelToString(int logLevel);

bool cli_copydb_getenv_source_pguri(char **pguri);
bool cli_copydb_getenv_split(SplitTableLargerThan *splitTablesLargerThan);

bool cli_copydb_getenv(CopyDBOptions *options);
bool cli_copydb_is_consistent(CopyDBOptions *options);
bool cli_read_previous_options(CopyDBOptions *options, CopyFilePaths *cfPaths);

bool cli_read_one_line(const char *filename,
					   const char *name,
					   char *target,
					   size_t size);

int cli_copy_db_getopts(int argc, char **argv);

bool cli_parse_bytes_pretty(const char *byteString,
							uint64_t *bytes,
							char *bytesPretty,
							size_t bytesPrettySize);

bool cli_prepare_pguris(ConnStrings *connStrings);

#endif  /* CLI_COMMON_H */
