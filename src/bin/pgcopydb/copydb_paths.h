/*
 * src/bin/pgcopydb/copydb_paths.h
 *	 SQL queries to discover the source database stream
 */

#ifndef COPYDB_PATHS_H
#define COPYDB_PATHS_H

#include <stdbool.h>

#include "pgsql.h"

/* we can inspect a work directory and discover previous run state */
typedef struct DirectoryState
{
	/* first, about the directory itself */
	bool directoryExists;
	bool directoryIsReady;

	bool schemaDumpIsDone;
	bool schemaPreDataHasBeenRestored;
	bool schemaPostDataHasBeenRestored;

	bool tableCopyIsDone;
	bool indexCopyIsDone;
	bool sequenceCopyIsDone;
	bool blobsCopyIsDone;

	bool allDone;
} DirectoryState;

/* track activity and allow resuming from a known state */
typedef struct CopyDoneFilePaths
{
	char preDataDump[MAXPGPATH];     /* /tmp/pgcopydb/run/dump-pre.done */
	char postDataDump[MAXPGPATH];    /* /tmp/pgcopydb/run/dump-post.done */
	char preDataRestore[MAXPGPATH];  /* /tmp/pgcopydb/run/restore-pre.done */
	char postDataRestore[MAXPGPATH]; /* /tmp/pgcopydb/run/restore-post.done */

	char tables[MAXPGPATH];     /* /tmp/pgcopydb/run/tables.done */
	char indexes[MAXPGPATH];    /* /tmp/pgcopydb/run/indexes.done */
	char sequences[MAXPGPATH];  /* /tmp/pgcopydb/run/sequences.done */
	char blobs[MAXPGPATH];      /* /tmp/pgcopydb/run/blobs.done */
} CopyDoneFilePaths;

/* Change Data Capture (logical decoding) paths */
typedef struct CDCPaths
{
	char dir[MAXPGPATH];              /* /tmp/pgcopydb/cdc */
	char originfile[MAXPGPATH];       /* /tmp/pgcopydb/cdc/origin */
	char slotfile[MAXPGPATH];         /* /tmp/pgcopydb/cdc/slot */
	char walsegsizefile[MAXPGPATH];   /* /tmp/pgcopydb/cdc/wal_segment_size */
	char tlifile[MAXPGPATH];          /* /tmp/pgcopydb/cdc/tli */
	char tlihistfile[MAXPGPATH];      /* /tmp/pgcopydb/cdc/tli.history */
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
	char rundir[MAXPGPATH];           /* /tmp/pgcopydb/run */

	CDCPaths cdc;
	CopyDoneFilePaths done;
	ComparePaths compare;
} CopyFilePaths;


/* the main pg_dump and pg_restore process are driven from split files */
typedef struct DumpPaths
{
	char rolesFilename[MAXPGPATH];   /* pg_dumpall --roles-only */
	char extnspFilename[MAXPGPATH];  /* pg_dump --schema-only -n ... */

	char preFilename[MAXPGPATH];     /* pg_dump --section=pre-data */
	char preListOutFilename[MAXPGPATH]; /* pg_restore --list */
	char preListFilename[MAXPGPATH]; /* pg_restore --use-list */

	char postFilename[MAXPGPATH];     /* pg_dump --section=post-data */
	char postListOutFilename[MAXPGPATH]; /* pg_restore --list */
	char postListFilename[MAXPGPATH];    /* pg_restore --use-list */
} DumpPaths;


#endif /* COPYDB_PATHS_H */
