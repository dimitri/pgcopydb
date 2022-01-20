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
#include "pgsql.h"

typedef struct CopyDBOptions
{
	char source_pguri[MAXCONNINFO];
	char target_pguri[MAXCONNINFO];
	int tableJobs;
	int indexJobs;
	bool dropIfExists;
	bool noOwner;
	bool skipLargeObjects;
} CopyDBOptions;


#endif  /* CLI_COPY_H */
