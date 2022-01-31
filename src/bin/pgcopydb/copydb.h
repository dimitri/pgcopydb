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


/* we can inspect a work directory and discover previous run state */
typedef struct DirectoryState
{
	/* first, about the directory itself */
	bool directoryExists;
	bool directoryIsReady;

	/* when we have a directory, what part of the job has been done? */
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


/* maintain all the internal paths we need in one place */
typedef struct CopyFilePaths
{
	char topdir[MAXPGPATH];           /* /tmp/pgcopydb */
	char pidfile[MAXPGPATH];          /* /tmp/pgcopydb/pgcopydb.pid */
	char schemadir[MAXPGPATH];        /* /tmp/pgcopydb/schema */
	char rundir[MAXPGPATH];           /* /tmp/pgcopydb/run */
	char tbldir[MAXPGPATH];           /* /tmp/pgcopydb/run/tables */
	char idxdir[MAXPGPATH];           /* /tmp/pgcopydb/run/indexes */

	CopyDoneFilePaths done;
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
	bool resume;

	char qname[NAMEDATALEN * 2 + 1];
	SourceTable sourceTable;
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
	DirectoryState dirState;

	char source_pguri[MAXCONNINFO];
	char target_pguri[MAXCONNINFO];

	TransactionSnapshot sourceSnapshot;

	CopyDataSection section;
	RestoreOptions restoreOptions;
	bool skipLargeObjects;

	bool restart;
	bool resume;

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


bool copydb_init_workdir(CopyDataSpec *copySpecs,
						 char *dir,
						 bool restart,
						 bool resume);

bool copydb_prepare_filepaths(CopyFilePaths *cfPaths, const char *topdir);
bool copydb_inspect_workdir(CopyFilePaths *cfPaths, DirectoryState *dirState);

bool copydb_rmdir_or_mkdir(const char *dir, bool removeDir);
bool copydb_prepare_dump_paths(CopyFilePaths *cfPaths, DumpPaths *dumpPaths);

bool copydb_init_specs(CopyDataSpec *specs,
					   char *source_pguri,
					   char *target_pguri,
					   int tableJobs,
					   int indexJobs,
					   CopyDataSection section,
					   RestoreOptions restoreOptions,
					   bool skipLargeObjects,
					   bool restart,
					   bool resume);

bool copydb_init_table_specs(CopyTableDataSpec *tableSpecs,
							 CopyDataSpec *specs,
							 SourceTable *source);

bool copydb_export_snapshot(TransactionSnapshot *snapshot);
bool copydb_set_snapshot(TransactionSnapshot *snapshot);
bool copydb_close_snapshot(TransactionSnapshot *snapshot);

bool copydb_prepare_table_specs(CopyDataSpec *specs);

bool copydb_start_vacuum_table(CopyTableDataSpec *tableSpecs);

bool copydb_fatal_exit(void);
bool copydb_wait_for_subprocesses(void);
bool copydb_collect_finished_subprocesses(void);


/* indexes.c */
bool copydb_init_indexes_paths(CopyFilePaths *cfPaths,
							   SourceIndexArray *indexArray,
							   IndexFilePathsArray *indexPathsArray);

bool copydb_copy_all_indexes(CopyDataSpec *specs);

bool copydb_start_index_processes(CopyDataSpec *specs,
								  SourceIndexArray *indexArray,
								  IndexFilePathsArray *indexPathsArray);

bool copydb_start_index_process(CopyDataSpec *specs,
								SourceIndexArray *indexArray,
								IndexFilePathsArray *indexPathsArray,
								Semaphore *lockFileSemaphore);

bool copydb_create_index(const char *pguri,
						 SourceIndex *index,
						 IndexFilePaths *indexPaths,
						 Semaphore *lockFileSemaphore,
						 Semaphore *createIndexSemaphore,
						 bool ifNotExists);


bool copydb_index_is_being_processed(SourceIndex *index,
									 IndexFilePaths *indexPaths,
									 Semaphore *lockFileSemaphore,
									 CopyIndexSummary *summary,
									 bool *isDone,
									 bool *isBeingProcessed);

bool copydb_mark_index_as_done(SourceIndex *index,
							   IndexFilePaths *indexPaths,
							   Semaphore *lockFileSemaphore,
							   CopyIndexSummary *summary);

bool copydb_prepare_create_index_command(SourceIndex *index,
										 bool ifNotExists,
										 char *command,
										 size_t size);

/* dump_restore.c */
bool copydb_dump_source_schema(CopyDataSpec *specs,
							   const char *snapshot,
							   PostgresDumpSection section);
bool copydb_target_prepare_schema(CopyDataSpec *specs);
bool copydb_target_finalize_schema(CopyDataSpec *specs);

bool copydb_objectid_has_been_processed_already(CopyDataSpec *specs,
												uint32_t oid);

/* sequence.c */
bool copydb_copy_all_sequences(CopyDataSpec *specs);

/* table-data.c */
bool copydb_copy_all_table_data(CopyDataSpec *specs);

bool copydb_start_table_processes(CopyDataSpec *specs);
bool copydb_start_table_process(CopyDataSpec *specs);

bool copydb_copy_table(CopyTableDataSpec *tableSpecs);

bool copydb_copy_table_indexes(CopyTableDataSpec *tableSpecs);
bool copydb_start_create_table_indexes(CopyTableDataSpec *tableSpecs);

bool copydb_create_constraints(CopyTableDataSpec *tableSpecs);

bool copydb_table_is_being_processed(CopyDataSpec *specs,
									 CopyTableDataSpec *tableSpecs,
									 bool *isDone,
									 bool *isBeingProcessed);

bool copydb_mark_table_as_done(CopyDataSpec *specs,
							   CopyTableDataSpec *tableSpecs);


/* summary.c */
bool prepare_summary_table(Summary *summary, CopyDataSpec *specs);
bool print_summary(Summary *summary, CopyDataSpec *specs);

#endif  /* COPYDB_H */
