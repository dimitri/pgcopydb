/*
 * src/bin/pgcopydb/cli_dump.h
 *     Implementation of a CLI which lets you run individual routines
 *     directly
 */

#ifndef CLI_DUMP_H
#define CLI_DUMP_H

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>

#include "cli_common.h"
#include "cli_root.h"
#include "pgsql.h"

typedef struct DumpDBOptions
{
	char source_pguri[MAXCONNINFO];
	char target_dir[MAXPGPATH];
	bool restart;
	bool resume;
	bool notConsistent;
	char snapshot[BUFSIZE];
} DumpDBOptions;


#endif  /* CLI_DUMP_H */
