/*
 * src/bin/pgcopydb/cli_stream.h
 *     Implementation of a CLI which lets you run individual routines
 *     directly
 */

#ifndef CLI_STREAM_H
#define CLI_STREAM_H

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>

#include "cli_common.h"
#include "cli_root.h"
#include "pgsql.h"

typedef struct StreamDBOptions
{
	char source_pguri[MAXCONNINFO];
	char target_pguri[MAXCONNINFO];
	char slotName[NAMEDATALEN];
} StreamDBOptions;


#endif  /* CLI_STREAM_H */
