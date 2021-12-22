/*
 * src/bin/pgcopydb/copydb.h
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#ifndef COPYDB_H
#define COPYDB_H

#include "pgcmd.h"

typedef struct CopyFilePaths
{
	char topdir[MAXPGPATH];		      /* /tmp/pgcopydb */
	char pidfile[MAXPGPATH];		  /* /tmp/pgcopydb/pgcopydb.pid */
	char schemadir[MAXPGPATH];	      /* /tmp/pgcopydb/schema */
	char rundir[MAXPGPATH];		      /* /tmp/pgcopydb/run */
	char tbldir[MAXPGPATH];		      /* /tmp/pgcopydb/run/tables */
	char idxfilepath[MAXPGPATH];	  /* /tmp/pgcopydb/run/indexes.json */
	char listdonefilepath[MAXPGPATH]; /* /tmp/pgcopydb/objects.list */
} CopyFilePaths;


bool copydb_init_workdir(CopyFilePaths *cfPaths, char *dir);

bool copydb_dump_source_schema(PostgresPaths *pgPaths,
							   CopyFilePaths *cfPaths,
							   const char *pguri);

bool copydb_target_prepare_schema(PostgresPaths *pgPaths,
								  CopyFilePaths *cfPaths,
								  const char *pguri);

#endif  /* COPYDB_H */
