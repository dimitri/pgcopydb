/*
 * src/bin/pgcopydb/copydb_paths.h
 *	 SQL queries to discover the source database stream
 */

#ifndef COPYDB_PATHS_H
#define COPYDB_PATHS_H

#include <stdbool.h>

#include "pgsql.h"

/* Change Data Capture (logical decoding) paths */
typedef struct CDCPaths
{
	char dir[MAXPGPATH];              /* /tmp/pgcopydb/cdc */
	char originfile[MAXPGPATH];       /* /tmp/pgcopydb/cdc/origin */
	char slotfile[MAXPGPATH];         /* /tmp/pgcopydb/cdc/slot */
	char walsegsizefile[MAXPGPATH];   /* /tmp/pgcopydb/cdc/wal_segment_size */
	char tlifile[MAXPGPATH];          /* /tmp/pgcopydb/cdc/tli */
	char lsntrackingfile[MAXPGPATH];  /* /tmp/pgcopydb/cdc/lsn.json */
} CDCPaths;


/* Compare Paths */
typedef struct ComparePaths
{
	char dir[MAXPGPATH];          /* /tmp/pgcopydb/compare */
	char sschemafile[MAXPGPATH]; /* /tmp/pgcopydb/compare/source-schema.json */
	char tschemafile[MAXPGPATH];  /* /tmp/pgcopydb/compare/target-schema.json */
	char sdatafile[MAXPGPATH];    /* /tmp/pgcopydb/compare/source-data.json */
	char tdatafile[MAXPGPATH];    /* /tmp/pgcopydb/compare/target-data.json */
} ComparePaths;


/* maintain all the internal paths we need in one place */
typedef struct CopyFilePaths
{
	char topdir[MAXPGPATH];           /* /tmp/pgcopydb */
	char pidfile[MAXPGPATH];          /* /tmp/pgcopydb/pgcopydb.pid */
	char spidfile[MAXPGPATH];         /* /tmp/pgcopydb/pgcopydb.service.pid */
	char sdbfile[MAXPGPATH];          /* /tmp/pgcopydb/schema/source.db */
	char fdbfile[MAXPGPATH];          /* /tmp/pgcopydb/schema/filter.db */
	char tdbfile[MAXPGPATH];          /* /tmp/pgcopydb/schema/target.db */
	char snfile[MAXPGPATH];           /* /tmp/pgcopydb/snapshot */
	char schemadir[MAXPGPATH];        /* /tmp/pgcopydb/schema */
	char schemafile[MAXPGPATH];       /* /tmp/pgcopydb/schema.json */
	char summaryfile[MAXPGPATH];      /* /tmp/pgcopydb/summary.json */

	CDCPaths cdc;
	ComparePaths compare;
} CopyFilePaths;


/* the main pg_dump and pg_restore process are driven from split files */
typedef struct DumpPaths
{
	char rolesFilename[MAXPGPATH];   /* pg_dumpall --roles-only */
	char extnspFilename[MAXPGPATH];  /* pg_dump --schema-only -n ... */

	char dumpFilename[MAXPGPATH];     /* pg_dump --section=pre-data --section=post-data */

	char preListOutFilename[MAXPGPATH]; /* pg_restore --list */
	char preListFilename[MAXPGPATH]; /* pg_restore --use-list */

	char postListOutFilename[MAXPGPATH]; /* pg_restore --list */
	char postListFilename[MAXPGPATH];    /* pg_restore --use-list */
} DumpPaths;


#endif /* COPYDB_PATHS_H */
