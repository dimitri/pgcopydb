/*
 * src/bin/pgcopydb/cli_restore.h
 *     Implementation of a CLI which lets you run individual routines
 *     directly
 */

#ifndef CLI_RESTORE_H
#define CLI_RESTORE_H

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>

#include "cli_common.h"
#include "cli_root.h"
#include "pgsql.h"

typedef struct RestoreDBOptions
{
	char source_dir[MAXPGPATH];
	char target_pguri[MAXCONNINFO];
	bool dropIfExists;
	bool noOwner;
} RestoreDBOptions;


#endif  /* CLI_RESTORE_H */
