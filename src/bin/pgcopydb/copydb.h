/*
 * src/bin/pgcopydb/copydb.h
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#ifndef COPYDB_H
#define COPYDB_H

#include "lock_utils.h"
#include "pgcmd.h"
#include "pgsql.h"
#include "schema.h"
#include "summary.h"


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
} CopyFilePaths;


/* the main pg_dump and pg_restore process are driven from split files */
typedef struct DumpPaths
{
	char preFilename[MAXPGPATH];  /* pg_dump --section=pre-data */
	char postFilename[MAXPGPATH]; /* pg_dump --section=post-data */
	char listFilename[MAXPGPATH]; /* pg_restore --list */
} DumpPaths;


/* per-table file paths */
typedef struct TableFilePaths
{
	char lockFile[MAXPGPATH];    /* table lock file */
	char doneFile[MAXPGPATH];    /* table done file (summary) */
	char idxListFile[MAXPGPATH]; /* index oids list file */
} TableFilePaths;


/* per-index file paths */
typedef struct IndexFilePaths
{
	char lockFile[MAXPGPATH];           /* index lock file */
	char doneFile[MAXPGPATH];           /* index done file (summary) */
	char constraintDoneFile[MAXPGPATH]; /* constraint done file */
} IndexFilePaths;

typedef struct IndexFilePathsArray
{
	int count;
	IndexFilePaths *array;      /* malloc'ed area */
} IndexFilePathsArray;


/*
 * pgcopydb uses Postgres facility to export snapshot and re-use them in other
 * transactions to use a consistent view of the data on the source database.
 */
typedef struct TransactionSnapshot
{
	PGSQL pgsql;
	char pguri[MAXCONNINFO];
	ConnectionType connectionType;
	char snapshot[BUFSIZE];
} TransactionSnapshot;


/*
 * pgcopydb relies on pg_dump and pg_restore to implement the pre-data and the
 * post-data section of the operation, and implements the data section
 * differently. The data section itself is actually split in separate steps.
 */
typedef enum
{
	DATA_SECTION_NONE = 0,
	DATA_SECTION_TABLE_DATA,
	DATA_SECTION_SET_SEQUENCES,
	DATA_SECTION_INDEXES,
	DATA_SECTION_CONSTRAINTS,
	DATA_SECTION_VACUUM,
	DATA_SECTION_ALL
} CopyDataSection;

/* all that's needed to drive a single TABLE DATA copy process */
typedef struct CopyTableDataSpec
{
	CopyFilePaths *cfPaths;
	PostgresPaths *pgPaths;

	char source_pguri[MAXCONNINFO];
	char target_pguri[MAXCONNINFO];

	TransactionSnapshot sourceSnapshot;

	CopyDataSection section;

	char qname[NAMEDATALEN * 2 + 1];
	SourceTable *sourceTable;
	CopyTableSummary *summary;
	SourceIndexArray *indexArray;

	int tableJobs;
	int indexJobs;
	Semaphore *indexSemaphore;  /* pointer to the main specs semaphore */

	TableFilePaths tablePaths;
	IndexFilePathsArray indexPathsArray;
} CopyTableDataSpec;


typedef struct CopyTableDataSpecsArray
{
	int count;
	CopyTableDataSpec *array;   /* malloc'ed area */
} CopyTableDataSpecsArray;


/* all that's needed to start a TABLE DATA copy for a whole database */
typedef struct CopyDataSpec
{
	CopyFilePaths cfPaths;
	PostgresPaths pgPaths;

	char source_pguri[MAXCONNINFO];
	char target_pguri[MAXCONNINFO];

	TransactionSnapshot sourceSnapshot;

	CopyDataSection section;
	bool dropIfExists;
	bool noOwner;
	bool skipLargeObjects;

	int tableJobs;
	int indexJobs;
	Semaphore tableSemaphore;
	Semaphore indexSemaphore;

	DumpPaths dumpPaths;
	CopyTableDataSpecsArray tableSpecsArray;
} CopyDataSpec;


/* specify section of a dump: pre-data, post-data, data, schema */
typedef enum
{
	PG_DUMP_SECTION_ALL = 0,
	PG_DUMP_SECTION_SCHEMA,
	PG_DUMP_SECTION_PRE_DATA,
	PG_DUMP_SECTION_POST_DATA,
	PG_DUMP_SECTION_DATA
} PostgresDumpSection;


bool copydb_init_workdir(CopyFilePaths *cfPaths, char *dir, bool removeDir);

bool copydb_init_specs(CopyDataSpec *specs,
					   char *source_pguri,
					   char *target_pguri,
					   int tableJobs,
					   int indexJobs,
					   CopyDataSection section,
					   bool dropIfExists,
					   bool noOwner,
					   bool skipLargeObjects);

bool copydb_init_table_specs(CopyTableDataSpec *tableSpecs,
							 CopyDataSpec *specs,
							 SourceTable *source);

bool copydb_init_indexes_paths(CopyTableDataSpec *tableSpecs);

bool copydb_dump_source_schema(CopyDataSpec *specs, PostgresDumpSection section);
bool copydb_target_prepare_schema(CopyDataSpec *specs);
bool copydb_target_finalize_schema(CopyDataSpec *specs);

bool copydb_objectid_has_been_processed_already(CopyDataSpec *specs,
												uint32_t oid);

bool copydb_export_snapshot(TransactionSnapshot *snapshot);
bool copydb_set_snapshot(TransactionSnapshot *snapshot);
bool copydb_close_snapshot(TransactionSnapshot *snapshot);

bool copydb_copy_all_sequences(CopyDataSpec *specs);

bool copydb_copy_all_table_data(CopyDataSpec *specs);
bool copydb_start_table_processes(CopyDataSpec *specs);
bool copydb_start_table_process(CopyDataSpec *specs);

bool copydb_copy_table(CopyTableDataSpec *tableSpecs);
bool copydb_copy_table_indexes(CopyTableDataSpec *tableSpecs);

bool copydb_start_create_indexes(CopyTableDataSpec *tableSpecs);
bool copydb_create_index(CopyTableDataSpec *tableSpecs, int idx);
bool copydb_create_constraints(CopyTableDataSpec *tableSpecs);

bool copydb_start_vacuum_table(CopyTableDataSpec *tableSpecs);

bool copydb_fatal_exit(void);
bool copydb_wait_for_subprocesses(void);
bool copydb_collect_finished_subprocesses(void);

bool prepare_summary_table(Summary *summary, CopyDataSpec *specs);
bool print_summary(Summary *summary, CopyDataSpec *specs);

#endif  /* COPYDB_H */
