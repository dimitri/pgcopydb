/*
 * src/bin/pg_autoctl/cli_common.h
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

#include "defaults.h"
#include "parson.h"
#include "pgcmd.h"
#include "pgsql.h"

typedef struct CopyDBOptions
{
	char dir[MAXPGPATH];

	char source_pguri[MAXCONNINFO];
	char target_pguri[MAXCONNINFO];

	int tableJobs;
	int indexJobs;
	uint64_t splitTablesLargerThan;
	char splitTablesLargerThanPretty[NAMEDATALEN];

	RestoreOptions restoreOptions;

	bool roles;
	bool skipLargeObjects;
	bool skipExtensions;

	bool restart;
	bool resume;
	bool notConsistent;
	char snapshot[BUFSIZE];
	char origin[BUFSIZE];

	bool follow;
	bool createSlot;
	bool currentpos;
	uint64_t endpos;
	uint64_t startpos;

	char filterFileName[MAXPGPATH];
	char slotName[MAXPGPATH];
} CopyDBOptions;

extern bool outputJSON;
extern CopyDBOptions copyDBoptions;

void cli_help(int argc, char **argv);

int cli_print_version_getopts(int argc, char **argv);
void cli_print_version(int argc, char **argv);

void cli_pprint_json(JSON_Value *js);
char * logLevelToString(int logLevel);

bool cli_copydb_getenv(CopyDBOptions *options);
bool cli_copydb_is_consistent(CopyDBOptions *options);

int cli_copy_db_getopts(int argc, char **argv);

bool cli_parse_bytes_pretty(const char *byteString,
							uint64_t *bytes,
							char *bytesPretty,
							size_t bytesPrettySize);

#endif  /* CLI_COMMON_H */
