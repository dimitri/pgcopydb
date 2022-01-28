/*
 * src/bin/pgcopydb/cli_copy.h
 *     Implementation of a CLI which lets you run individual routines
 *     directly
 */

#ifndef CLI_COPY_H
#define CLI_COPY_H

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>

#include "cli_common.h"
#include "cli_root.h"
#include "pgcmd.h"
#include "pgsql.h"

typedef struct CopyDBOptions
{
	char source_pguri[MAXCONNINFO];
	char target_pguri[MAXCONNINFO];
	int tableJobs;
	int indexJobs;
	RestoreOptions restoreOptions;
	bool skipLargeObjects;
	bool restart;
	bool resume;
	bool notConsistent;
} CopyDBOptions;


#endif  /* CLI_COPY_H */
