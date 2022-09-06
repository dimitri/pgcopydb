/*
 * src/bin/pgcopydb/copydb.h
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#ifndef COPYDB_H
#define COPYDB_H

#include "filtering.h"
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

/* Change Data Capture (logical decoding) paths */
typedef struct CDCPaths
{
	char dir[MAXPGPATH];              /* /tmp/pgcopydb/cdc */
	char originfile[MAXPGPATH];       /* /tmp/pgcopydb/cdc/origin */
	char walsegsizefile[MAXPGPATH];   /* /tmp/pgcopydb/cdc/wal_segment_size */
	char tlifile[MAXPGPATH];          /* /tmp/pgcopydb/cdc/tli */
	char tlihistfile[MAXPGPATH];      /* /tmp/pgcopydb/cdc/tli.history */
} CDCPaths;

/* maintain all the internal paths we need in one place */
typedef struct CopyFilePaths
{
	char topdir[MAXPGPATH];           /* /tmp/pgcopydb */
	char pidfile[MAXPGPATH];          /* /tmp/pgcopydb/pgcopydb.pid */
	char snfile[MAXPGPATH];           /* /tmp/pgcopydb/snapshot */
	char schemadir[MAXPGPATH];        /* /tmp/pgcopydb/schema */
	char schemafile[MAXPGPATH];       /* /tmp/pgcopydb/schema.json */
	char rundir[MAXPGPATH];           /* /tmp/pgcopydb/run */
	char tbldir[MAXPGPATH];           /* /tmp/pgcopydb/run/tables */
	char idxdir[MAXPGPATH];           /* /tmp/pgcopydb/run/indexes */

	CDCPaths cdc;
	CopyDoneFilePaths done;
} CopyFilePaths;


/* the main pg_dump and pg_restore process are driven from split files */
typedef struct DumpPaths
{
	char rolesFilename[MAXPGPATH];   /* pg_dumpall --roles-only */

	char preFilename[MAXPGPATH];     /* pg_dump --section=pre-data */
	char preListFilename[MAXPGPATH]; /* pg_restore --list */

	char postFilename[MAXPGPATH];     /* pg_dump --section=post-data */
	char postListFilename[MAXPGPATH]; /* pg_restore --list */
} DumpPaths;


/* per-table file paths */
typedef struct TableFilePaths
{
	char lockFile[MAXPGPATH];    /* table lock file */
	char doneFile[MAXPGPATH];    /* table done file (summary) */
	char idxListFile[MAXPGPATH]; /* index oids list file */

	char truncateDoneFile[MAXPGPATH];    /* table truncate done file */
} TableFilePaths;


/* per-index file paths */
typedef struct IndexFilePaths
{
	char lockFile[MAXPGPATH];           /* index lock file */
	char doneFile[MAXPGPATH];           /* index done file (summary) */
	char constraintLockFile[MAXPGPATH]; /* constraint lock file */
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
typedef enum
{
	SNAPSHOT_STATE_UNKNOWN = 0,
	SNAPSHOT_STATE_SKIPPED,
	SNAPSHOT_STATE_NOT_CONSISTENT,
	SNAPSHOT_STATE_EXPORTED,
	SNAPSHOT_STATE_SET,
	SNAPSHOT_STATE_CLOSED
} TransactionSnapshotState;

typedef struct TransactionSnapshot
{
	TransactionSnapshotState state;
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
	DATA_SECTION_SCHEMA,
	DATA_SECTION_TABLE_DATA,
	DATA_SECTION_SET_SEQUENCES,
	DATA_SECTION_INDEXES,
	DATA_SECTION_CONSTRAINTS,
	DATA_SECTION_BLOBS,
	DATA_SECTION_VACUUM,
	DATA_SECTION_ALL
} CopyDataSection;

/* all that's needed to drive a single TABLE DATA copy process */
typedef struct CopyTableDataPartSpec
{
	int partNumber;
	int partCount;              /* zero when table is not partitionned */

	int64_t min;                /* WHERE partKey >= min */
	int64_t max;                /*   AND partKey  < max */

	char partKey[NAMEDATALEN];
	char copyQuery[BUFSIZE];    /* COPY (...) TO STDOUT */
} CopyTableDataPartSpec;


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
	Semaphore *truncateSemaphore;

	TableFilePaths tablePaths;
	IndexFilePathsArray indexPathsArray;

	/* same-table concurrency with COPY WHERE clause partitioning */
	CopyTableDataPartSpec part;
} CopyTableDataSpec;


typedef struct CopyTableDataSpecsArray
{
	int count;
	CopyTableDataSpec *array;   /* malloc'ed area */
} CopyTableDataSpecsArray;


/*
 * Build a hash-table of all the SQL level objects that we filter-out when
 * applying our filtering rules. We need to find those objects again when
 * parsing the pg_restore --list output, where we almost always have the object
 * Oid, but sometimes have to use the "schema name owner" format instead, as in
 * the following pg_restore --list example output:
 *
 *   3310; 0 0 INDEX ATTACH public payment_p2020_06_customer_id_idx postgres
 */
typedef enum
{
	OBJECT_KIND_UNKNOWN = 0,
	OBJECT_KIND_TABLE,
	OBJECT_KIND_INDEX,
	OBJECT_KIND_CONSTRAINT,
	OBJECT_KIND_SEQUENCE
} ObjectKind;


typedef struct SourceFilterItem
{
	uint32_t oid;

	ObjectKind kind;

	/* it's going to be only one of those, depending on the object kind */
	SourceTable table;
	SourceSequence sequence;
	SourceIndex index;

	/* schema - name - owner */
	char restoreListName[RESTORE_LIST_NAMEDATALEN];

	UT_hash_handle hOid;            /* makes this structure hashable */
	UT_hash_handle hName;           /* makes this structure hashable */
} SourceFilterItem;


/* all that's needed to start a TABLE DATA copy for a whole database */
typedef struct CopyDataSpec
{
	CopyFilePaths cfPaths;
	PostgresPaths pgPaths;
	DirectoryState dirState;

	SourceFilters filters;
	SourceFilterItem *hOid;     /* hash table of objects, by Oid */
	SourceFilterItem *hName;    /* hash table of objects, by pg_restore name */

	char source_pguri[MAXCONNINFO];
	char target_pguri[MAXCONNINFO];

	TransactionSnapshot sourceSnapshot;

	CopyDataSection section;
	RestoreOptions restoreOptions;
	bool roles;
	bool skipLargeObjects;

	bool restart;
	bool resume;
	bool consistent;

	bool follow;                /* pgcopydb fork --follow */

	int tableJobs;
	int indexJobs;
	uint64_t splitTablesLargerThan;
	char splitTablesLargerThanPretty[NAMEDATALEN];

	Semaphore tableSemaphore;
	Semaphore indexSemaphore;

	DumpPaths dumpPaths;
	SourceTableArray sourceTableArray;
	SourceIndexArray sourceIndexArray;
	CopyTableDataSpecsArray tableSpecsArray;
	SourceSequenceArray sequenceArray;
} CopyDataSpec;


/* specify section of a dump: pre-data, post-data, data, schema */
typedef enum
{
	PG_DUMP_SECTION_ALL = 0,
	PG_DUMP_SECTION_SCHEMA,
	PG_DUMP_SECTION_PRE_DATA,
	PG_DUMP_SECTION_POST_DATA,
	PG_DUMP_SECTION_DATA,
	PG_DUMP_SECTION_ROLES       /* pg_dumpall --roles-only */
} PostgresDumpSection;


extern GUC srcSettings[];
extern GUC dstSettings[];

/* copydb.h */
void cli_copy_prepare_specs(CopyDataSpec *copySpecs, CopyDataSection section);

bool copydb_init_workdir(CopyDataSpec *copySpecs,
						 char *dir,
						 bool restart,
						 bool resume,
						 bool auxilliary);

bool copydb_prepare_filepaths(CopyFilePaths *cfPaths,
							  const char *topdir,
							  bool auxilliary);
bool copydb_inspect_workdir(CopyFilePaths *cfPaths, DirectoryState *dirState);

bool copydb_rmdir_or_mkdir(const char *dir, bool removeDir);
bool copydb_prepare_dump_paths(CopyFilePaths *cfPaths, DumpPaths *dumpPaths);

bool copydb_init_specs(CopyDataSpec *specs,
					   char *source_pguri,
					   char *target_pguri,
					   int tableJobs,
					   int indexJobs,
					   uint64_t splitTablesLargerThan,
					   char *splitTablesLargerThanPretty,
					   CopyDataSection section,
					   char *snapshot,
					   RestoreOptions restoreOptions,
					   bool roles,
					   bool skipLargeObjects,
					   bool restart,
					   bool resume,
					   bool consistent);

bool copydb_init_table_specs(CopyTableDataSpec *tableSpecs,
							 CopyDataSpec *specs,
							 SourceTable *source,
							 int partNumber);

bool copydb_init_tablepaths_for_part(CopyTableDataSpec *tableSpecs,
									 TableFilePaths *tablePaths,
									 int partNumber);

bool copydb_export_snapshot(TransactionSnapshot *snapshot);

bool copydb_copy_snapshot(CopyDataSpec *specs, TransactionSnapshot *snapshot);
bool copydb_prepare_snapshot(CopyDataSpec *copySpecs);
bool copydb_set_snapshot(CopyDataSpec *copySpecs);
bool copydb_close_snapshot(CopyDataSpec *copySpecs);

bool copydb_start_vacuum_table(CopyTableDataSpec *tableSpecs);

bool copydb_fatal_exit(void);
bool copydb_wait_for_subprocesses(void);
bool copydb_collect_finished_subprocesses(bool *allDone);

bool copydb_copy_roles(CopyDataSpec *copySpecs);

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
						 bool constraint,
						 bool ifNotExists);


bool copydb_index_is_being_processed(SourceIndex *index,
									 IndexFilePaths *indexPaths,
									 bool constraint,
									 Semaphore *lockFileSemaphore,
									 CopyIndexSummary *summary,
									 bool *isDone,
									 bool *isBeingProcessed);

bool copydb_mark_index_as_done(SourceIndex *index,
							   IndexFilePaths *indexPaths,
							   bool constraint,
							   Semaphore *lockFileSemaphore,
							   CopyIndexSummary *summary);

bool copydb_prepare_create_index_command(SourceIndex *index,
										 bool ifNotExists,
										 char *command,
										 size_t size);

bool copydb_prepare_create_constraint_command(SourceIndex *index,
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

bool copydb_write_restore_list(CopyDataSpec *specs, PostgresDumpSection section);

/* sequence.c */
bool copydb_copy_all_sequences(CopyDataSpec *specs);
bool copydb_prepare_sequence_specs(CopyDataSpec *specs, PGSQL *pgsql);

/* table-data.c */
bool copydb_fetch_schema_and_prepare_specs(CopyDataSpec *specs);
bool copydb_objectid_is_filtered_out(CopyDataSpec *specs,
									 uint32_t oid,
									 char *restoreListName);

bool copydb_prepare_table_specs(CopyDataSpec *specs, PGSQL *pgsql);
bool copydb_fetch_filtered_oids(CopyDataSpec *specs, PGSQL *pgsql);

char * copydb_ObjectKindToString(ObjectKind kind);

bool copydb_copy_all_table_data(CopyDataSpec *specs);

bool copydb_process_table_data(CopyDataSpec *specs);
bool copydb_process_table_data_worker(CopyDataSpec *specs);

bool copydb_copy_table(CopyDataSpec *specs, CopyTableDataSpec *tableSpecs);

bool copydb_copy_table_indexes(CopyTableDataSpec *tableSpecs);
bool copydb_create_table_indexes(CopyTableDataSpec *tableSpecs);

bool copydb_create_constraints(CopyTableDataSpec *tableSpecs);

bool copydb_table_is_being_processed(CopyDataSpec *specs,
									 CopyTableDataSpec *tableSpecs,
									 bool *isDone,
									 bool *isBeingProcessed);

bool copydb_mark_table_as_done(CopyDataSpec *specs,
							   CopyTableDataSpec *tableSpecs);

bool copydb_table_parts_are_all_done(CopyDataSpec *specs,
									 CopyTableDataSpec *tableSpecs,
									 bool *allPartsDone,
									 bool *isBeingProcessed);

bool copydb_start_blob_process(CopyDataSpec *specs);
bool copydb_copy_blobs(CopyDataSpec *specs);

/* summary.c */
bool prepare_summary_table(Summary *summary, CopyDataSpec *specs);
bool print_summary(Summary *summary, CopyDataSpec *specs);

#endif  /* COPYDB_H */
