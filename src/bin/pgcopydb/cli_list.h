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

typedef struct ListDBOptions
{
	char dir[MAXPGPATH];

	char source_pguri[MAXCONNINFO];
	char schema_name[NAMEDATALEN];
	char table_name[NAMEDATALEN];
	char filterFileName[MAXPGPATH];

	bool listSkipped;
	bool noPKey;
	bool cache;
	bool dropCache;

	uint64_t splitTablesLargerThan;
	char splitTablesLargerThanPretty[NAMEDATALEN];
} ListDBOptions;


#endif  /* CLI_LIST_H */
