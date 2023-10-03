/*
 * src/bin/pgcopydb/cli_list.h
 *     Implementation of a CLI which lets you run individual routines
 *     directly
 */

#ifndef CLI_LIST_H
#define CLI_LIST_H

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>

#include "cli_common.h"
#include "cli_root.h"
#include "pgsql.h"
#include "schema.h"

typedef struct ListDBOptions
{
	char dir[MAXPGPATH];

	ConnStrings connStrings;

	char schema_name[PG_NAMEDATALEN];
	char table_name[PG_NAMEDATALEN];
	char filterFileName[MAXPGPATH];

	bool listSkipped;
	bool noPKey;
	bool cache;
	bool dropCache;
	bool summary;
	bool availableVersions;
	bool requirements;

	SplitTableLargerThan splitTablesLargerThan;
} ListDBOptions;


#endif  /* CLI_LIST_H */
