/*
 * src/bin/pgcopydb/copydb.h
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#ifndef COPYDB_H
#define COPYDB_H

#include "pgcmd.h"
#include "schema.h"


/* maintain all the internal paths we need in one place */
typedef struct CopyFilePaths
{
	char topdir[MAXPGPATH];           /* /tmp/pgcopydb */
	char pidfile[MAXPGPATH];          /* /tmp/pgcopydb/pgcopydb.pid */
	char schemadir[MAXPGPATH];        /* /tmp/pgcopydb/schema */
	char rundir[MAXPGPATH];           /* /tmp/pgcopydb/run */
	char tbldir[MAXPGPATH];           /* /tmp/pgcopydb/run/tables */
	char idxdir[MAXPGPATH];           /* /tmp/pgcopydb/run/indexes */
	char idxfilepath[MAXPGPATH];      /* /tmp/pgcopydb/run/indexes.json */
	char listdonefilepath[MAXPGPATH]; /* /tmp/pgcopydb/objects.list */
} CopyFilePaths;


/* tracking sub-processes that are used for TABLE DATA copying */
typedef struct TableDataProcess
{
	pid_t pid;
	uint32_t oid;
	char lockFile[MAXPGPATH];   /* /tmp/pgcopydb/run/tables/{oid} */
	char doneFile[MAXPGPATH];   /* /tmp/pgcopydb/run/tables/{oid}.done */
} TableDataProcess;


typedef struct TableDataProcessArray
{
	int count;
	TableDataProcess *array;    /* malloc'ed area */
} TableDataProcessArray;


/* all that's needed to start a TABLE DATA copy for a whole database */
typedef struct CopyDataSpec
{
	CopyFilePaths *cfPaths;
	PostgresPaths *pgPaths;

	char *source_pguri;
	char *target_pguri;

	int tableJobs;
	int indexJobs;
} CopyDataSpec;


/* all that's needed to drive a single TABLE DATA copy process */
typedef struct CopyTableDataSpec
{
	CopyFilePaths *cfPaths;
	PostgresPaths *pgPaths;

	char *source_pguri;
	char *target_pguri;

	SourceTable *sourceTable;
	SourceIndexArray *indexArray;
	TableDataProcess *process;

	int tableJobs;
	int indexJobs;
} CopyTableDataSpec;

bool copydb_init_workdir(CopyFilePaths *cfPaths, char *dir);

bool copydb_dump_source_schema(PostgresPaths *pgPaths,
							   CopyFilePaths *cfPaths,
							   const char *pguri);

bool copydb_target_prepare_schema(PostgresPaths *pgPaths,
								  CopyFilePaths *cfPaths,
								  const char *pguri);

bool copydb_target_finalize_schema(PostgresPaths *pgPaths,
								   CopyFilePaths *cfPaths,
								   const char *pguri);

bool copydb_copy_all_table_data(CopyDataSpec *specs);
bool copydb_start_table_data(CopyTableDataSpec *spec);
bool copydb_copy_table(CopyTableDataSpec *tableSpecs);
bool copydb_start_create_indexes(CopyTableDataSpec *tableSpecs);
bool copydb_create_index(CopyTableDataSpec *tableSpecs, int idx);
bool copydb_create_constraints(CopyTableDataSpec *tableSpecs);

bool copydb_fatal_exit(TableDataProcessArray *subprocessArray);
bool copydb_wait_for_subprocesses(void);

#endif  /* COPYDB_H */
