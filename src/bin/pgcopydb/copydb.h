/*
 * src/bin/pgcopydb/copydb.h
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#ifndef COPYDB_H
#define COPYDB_H

#include "cli_common.h"
#include "copydb_paths.h"
#include "filtering.h"
#include "lock_utils.h"
#include "queue_utils.h"
#include "pgcmd.h"
#include "pgsql.h"
#include "schema.h"
#include "summary.h"


/*
 * These GUC settings are set with the SET command, and are meant to be
 * controled by pgcopydb without a way for the user to override them.
 */
#define COMMON_GUC_SETTINGS \
	{ "client_encoding", "'UTF-8'" }, \
	{ "extra_float_digits", "3" }, \
	{ "statement_timeout", "0" }, \
	{ "default_transaction_read_only", "off" }

/*
 * These parameters are added to the connection strings, unless the user has
 * added them, allowing user-defined values to be taken into account.
 */
extern KeyVal connStringDefaults;


/*
 * pgcopydb creates System V OS level objects such as message queues and
 * semaphores, and those have to be cleaned-up "manually".
 */
#define SYSV_RES_MAX_COUNT 16

typedef enum
{
	SYSV_UNKNOWN = 0,
	SYSV_QUEUE,
	SYSV_SEMAPHORE
} SysVKind;

typedef struct SysVRes
{
	SysVKind kind;
	bool unlinked;

	union res
	{
		Queue queue;
		Semaphore semaphore;
	} res;
} SysVRes;

typedef struct SysVResArray
{
	int count;
	SysVRes array[SYSV_RES_MAX_COUNT];
} SysVResArray;

extern SysVResArray system_res_array;

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

typedef enum
{
	SNAPSHOT_KIND_UNKNOWN = 0,
	SNAPSHOT_KIND_SQL,
	SNAPSHOT_KIND_LOGICAL
} TransactionSnapshotKind;

typedef struct TransactionSnapshot
{
	TransactionSnapshotKind kind;
	TransactionSnapshotState state;

	char *pguri;    /* malloc'ed area */
	SafeURI safeURI;

	PGSQL pgsql;

	bool exportedCreateSlotSnapshot;
	LogicalStreamClient stream;

	ConnectionType connectionType;

	char snapshot[BUFSIZE];

	/* indicator for read-only source db */
	bool isReadOnly;
} TransactionSnapshot;


/* all that's needed to drive a single TABLE DATA copy process */
typedef struct CopyTableDataPartSpec
{
	int partNumber;
	int partCount;              /* zero when table is not partitionned */

	int64_t min;                /* WHERE partKey >= min */
	int64_t max;                /*   AND partKey  < max */

	char partKey[PG_NAMEDATALEN];
} CopyTableDataPartSpec;


typedef struct CopyTableDataSpec
{
	CopyFilePaths *cfPaths;
	PostgresPaths *pgPaths;

	ConnStrings *connStrings;

	CopyDataSection section;
	bool resume;

	SourceTable *sourceTable;
	CopyTableSummary summary;
	CopyVacuumTableSummary vSummary;
	CopyArgs copyArgs;

	int tableJobs;
	int indexJobs;

	/* same-table concurrency with COPY WHERE clause partitioning */
	CopyTableDataPartSpec part;

	/* summary/activity tracking */
	uint32_t countPartsDone;
	pid_t partsDonePid;
	bool allPartsAreDone;

	uint32_t countIndexesLeft;
	pid_t indexesDonePid;
	bool allIndexesAreDone;
} CopyTableDataSpec;


typedef struct CopyIndexSpec
{
	SourceIndex *sourceIndex;
	CopyIndexSummary summary;
} CopyIndexSpec;


/*
 * Extensions versions to install on the target can be specified by the user.
 */
typedef struct ExtensionReqs
{
	char extname[PG_NAMEDATALEN];
	char version[BUFSIZE];

	UT_hash_handle hh;          /* makes this structure hashable */
} ExtensionReqs;

/*
 * pgcopydb sentinel is a table that's created on the source catalog and allows
 * communicating elements from the outside, and in between the receive and
 * apply processes.
 */
typedef struct CopyDBSentinel
{
	bool apply;
	uint64_t startpos;
	uint64_t endpos;
	uint64_t write_lsn;
	uint64_t flush_lsn;
	uint64_t replay_lsn;
} CopyDBSentinel;


/* we can inspect the source catalogs and discover previous run state */
typedef struct PreviousRunState
{
	bool schemaDumpIsDone;
	bool schemaPreDataHasBeenRestored;
	bool schemaPostDataHasBeenRestored;

	bool tableCopyIsDone;
	bool indexCopyIsDone;
	bool sequenceCopyIsDone;
	bool blobsCopyIsDone;

	bool allDone;
} PreviousRunState;


/* all that's needed to start a TABLE DATA copy for a whole database */
typedef struct CopyDataSpec
{
	CopyFilePaths cfPaths;
	PostgresPaths pgPaths;
	PreviousRunState runState;

	SourceFilters filters;

	ExtensionReqs *extRequirements;

	ConnStrings connStrings;
	TransactionSnapshot sourceSnapshot;

	CopyDataSection section;
	RestoreOptions restoreOptions;
	bool roles;
	bool skipLargeObjects;
	bool skipExtensions;
	bool skipCommentOnExtension;
	bool skipCollations;
	bool skipVacuum;
	bool skipAnalyze;
	bool skipDBproperties;
	bool skipCtidSplit;
	bool noRolesPasswords;
	bool useCopyBinary;

	bool restart;
	bool resume;
	bool consistent;
	bool failFast;

	bool fetchCatalogs;         /* cache invalidation of local catalogs db */
	bool fetchFilteredOids;     /* allow bypassing dump/restore filter prep */

	bool follow;                /* pgcopydb fork --follow */

	int tableJobs;
	int indexJobs;
	int vacuumJobs;
	int lObjectJobs;

	SplitTableLargerThan splitTablesLargerThan;
	int splitMaxParts;
	bool estimateTableSizes;

	Queue copyQueue;
	Queue indexQueue;
	Queue vacuumQueue;
	Queue loQueue;

	DumpPaths dumpPaths;

	/* results from calling has_database_privilege() on the source */
	bool hasDBCreatePrivilege;
	bool hasDBTempPrivilege;

	Catalogs catalogs;
} CopyDataSpec;

extern GUC srcSettings95[];
extern GUC srcSettings[];
extern GUC dstSettings[];

/* copydb.h */
void cli_copy_prepare_specs(CopyDataSpec *copySpecs, CopyDataSection section);

bool copydb_init_workdir(CopyDataSpec *copySpecs,
						 char *dir,
						 bool service,
						 char *serviceName,
						 bool restart,
						 bool resume,
						 bool createWorkDir);

bool copydb_acquire_pidfile(CopyFilePaths *cfPaths, char *serviceName);
bool copydb_create_pidfile(const char *pidfile, pid_t pid, bool createPidFile);

bool copydb_prepare_filepaths(CopyFilePaths *cfPaths,
							  const char *topdir,
							  const char *serviceName);

bool copydb_inspect_workdir(CopyFilePaths *cfPaths);

bool copydb_rmdir_or_mkdir(const char *dir, bool removeDir);
bool copydb_prepare_dump_paths(CopyFilePaths *cfPaths, DumpPaths *dumpPaths);

bool copydb_init_specs(CopyDataSpec *specs,
					   CopyDBOptions *options,
					   CopyDataSection section);

bool copydb_init_table_specs(CopyTableDataSpec *tableSpecs,
							 CopyDataSpec *specs,
							 SourceTable *source,
							 int partNumber);

bool copydb_export_snapshot(TransactionSnapshot *snapshot);

bool copydb_fatal_exit(void);
bool copydb_wait_for_subprocesses(bool failFast);

bool copydb_register_sysv_semaphore(SysVResArray *array, Semaphore *semaphore);
bool copydb_register_sysv_queue(SysVResArray *array, Queue *queue);

bool copydb_unlink_sysv_semaphore(SysVResArray *array, Semaphore *semaphore);
bool copydb_unlink_sysv_queue(SysVResArray *array, Queue *queue);

bool copydb_cleanup_sysv_resources(SysVResArray *array);

/* catalog.c */
bool catalog_init_from_specs(CopyDataSpec *copySpecs);
bool catalog_open_from_specs(CopyDataSpec *copySpecs);
bool catalog_close_from_specs(CopyDataSpec *copySpecs);
bool catalog_register_setup_from_specs(CopyDataSpec *copySpecs);
bool catalog_update_setup(CopyDataSpec *copySpecs);

/* snapshot.c */
bool copydb_copy_snapshot(CopyDataSpec *specs, TransactionSnapshot *snapshot);
bool copydb_prepare_snapshot(CopyDataSpec *copySpecs);
bool copydb_should_export_snapshot(CopyDataSpec *copySpecs);
bool copydb_set_snapshot(CopyDataSpec *copySpecs);
bool copydb_close_snapshot(CopyDataSpec *copySpecs);

bool copydb_create_logical_replication_slot(CopyDataSpec *copySpecs,
											const char *logrep_pguri,
											ReplicationSlot *slot);

bool snapshot_write_slot(const char *filename, ReplicationSlot *slot);
bool snapshot_read_slot(const char *filename, ReplicationSlot *slot);

/* extensions.c */
bool copydb_start_extension_data_process(CopyDataSpec *specs,
										 bool createExtensions);
bool copydb_copy_extensions(CopyDataSpec *copySpecs, bool createExtensions);

bool copydb_parse_extensions_requirements(CopyDataSpec *copySpecs,
										  char *filename);

/* indexes.c */
bool copydb_start_index_supervisor(CopyDataSpec *specs);
bool copydb_index_supervisor(CopyDataSpec *specs);
bool copydb_start_index_workers(CopyDataSpec *specs);
bool copydb_index_worker(CopyDataSpec *specs);
bool copydb_create_index_by_oid(CopyDataSpec *specs, PGSQL *dst, uint32_t indexOid);

bool copydb_add_table_indexes(CopyDataSpec *specs,
							  CopyTableDataSpec *tableSpecs);

bool copydb_index_workers_send_stop(CopyDataSpec *specs);

bool copydb_table_indexes_are_done(CopyDataSpec *specs,
								   SourceTable *table,
								   bool *indexesAreDone,
								   bool *constraintsAreBeingBuilt);

bool copydb_copy_all_indexes(CopyDataSpec *specs);

bool copydb_create_index(CopyDataSpec *specs,
						 PGSQL *dst,
						 SourceIndex *index,
						 bool ifNotExists);

bool copydb_index_is_being_processed(CopyDataSpec *specs,
									 CopyIndexSpec *indexSpecs,
									 bool *isDone);

bool copydb_mark_index_as_done(CopyDataSpec *specs, CopyIndexSpec *indexSpecs);

bool copydb_prepare_create_index_command(CopyIndexSpec *indexSpecs,
										 bool ifNotExists);

bool copydb_prepare_create_constraint_command(CopyIndexSpec *indexSpecs);

bool copydb_create_constraints(CopyDataSpec *spec, PGSQL *dst, SourceTable *table);

/* dump_restore.c */
bool copydb_dump_source_schema(CopyDataSpec *specs, const char *snapshot);
bool copydb_target_prepare_schema(CopyDataSpec *specs);
bool copydb_copy_database_properties(CopyDataSpec *specs);
bool copydb_target_drop_tables(CopyDataSpec *specs);
bool copydb_target_finalize_schema(CopyDataSpec *specs);

bool copydb_objectid_has_been_processed_already(CopyDataSpec *specs,
												ArchiveContentItem *item);

bool copydb_write_restore_list(CopyDataSpec *specs, PostgresDumpSection section);

/* sequences.c */
bool copydb_copy_all_sequences(CopyDataSpec *specs, bool reset);
bool copydb_start_seq_process(CopyDataSpec *specs);
bool copydb_prepare_sequence_specs(CopyDataSpec *specs, PGSQL *pgsql, bool reset);

/* copydb_schema.c */
bool copydb_fetch_schema_and_prepare_specs(CopyDataSpec *specs);
bool copydb_objectid_is_filtered_out(CopyDataSpec *specs,
									 uint32_t oid,
									 char *restoreListName);
bool copydb_matview_refresh_is_filtered_out(CopyDataSpec *specs,
											uint32_t oid);

bool copydb_prepare_table_specs(CopyDataSpec *specs, PGSQL *pgsql);
bool copydb_prepare_index_specs(CopyDataSpec *specs, PGSQL *pgsql);
bool copydb_prepare_namespace_specs(CopyDataSpec *specs, PGSQL *pgsql);
bool copydb_fetch_filtered_oids(CopyDataSpec *specs, PGSQL *pgsql);

bool copydb_prepare_target_catalog(CopyDataSpec *specs);
bool copydb_schema_already_exists(CopyDataSpec *specs,
								  uint32_t sourceOid,
								  bool *exists);

/* table-data.c */
bool copydb_copy_all_table_data(CopyDataSpec *specs);
bool copydb_process_table_data(CopyDataSpec *specs);

bool copydb_start_copy_supervisor(CopyDataSpec *specs);
bool copydb_copy_supervisor(CopyDataSpec *specs);
bool copydb_copy_start_worker_queue_tables(CopyDataSpec *specs);
bool copydb_copy_worker_queue_tables(CopyDataSpec *specs);
bool copydb_copy_supervisor_send_stop(CopyDataSpec *specs);
bool copydb_start_table_data_workers(CopyDataSpec *specs);
bool copydb_table_data_worker(CopyDataSpec *specs);

bool copydb_add_copy(CopyDataSpec *specs, uint32_t oid, uint32_t part);
bool copydb_copy_data_by_oid(CopyDataSpec *specs, PGSQL *src,
							 PGSQL *dst, uint32_t oid, uint32_t part);

bool copydb_process_table_data_worker(CopyDataSpec *specs);

bool copydb_process_table_data_with_workers(CopyDataSpec *specs);

bool copydb_copy_table(CopyDataSpec *specs, PGSQL *src, PGSQL *dst,
					   CopyTableDataSpec *tableSpecs);


bool copydb_table_create_lockfile(CopyDataSpec *specs,
								  CopyTableDataSpec *tableSpecs,
								  PGSQL *dst,
								  bool *isDone);

bool copydb_mark_table_as_done(CopyDataSpec *specs,
							   CopyTableDataSpec *tableSpecs);

bool copydb_table_parts_are_all_done(CopyDataSpec *specs,
									 CopyTableDataSpec *tableSpecs,
									 bool *allPartsDone,
									 bool *isBeingProcessed);

bool copydb_prepare_copy_query(CopyTableDataSpec *tableSpecs, CopyArgs *args);

bool copydb_prepare_summary_command(CopyTableDataSpec *tableSpecs);

bool copydb_check_table_exists(PGSQL *pgsql, SourceTable *table, bool *exists);

/* blobs.c */
bool copydb_start_blob_process(CopyDataSpec *specs);

bool copydb_has_large_objects(CopyDataSpec *specs, bool *hasLargeObjects);

bool copydb_blob_supervisor(CopyDataSpec *specs);
bool copydb_start_blob_workers(CopyDataSpec *specs);
bool copydb_blob_worker(CopyDataSpec *specs);
bool copydb_queue_largeobject_metadata(CopyDataSpec *specs, uint64_t *count);
bool copydb_copy_blob_by_oid(CopyDataSpec *specs, uint32_t oid);
bool copydb_add_blob(CopyDataSpec *specs, uint32_t oid);
bool copydb_send_lo_stop(CopyDataSpec *specs);

/* vacuum.c */
bool vacuum_start_supervisor(CopyDataSpec *specs);
bool vacuum_supervisor(CopyDataSpec *specs);
bool vacuum_start_workers(CopyDataSpec *specs);
bool vacuum_worker(CopyDataSpec *specs);
bool vacuum_analyze_table_by_oid(CopyDataSpec *specs, uint32_t oid);
bool vacuum_add_table(CopyDataSpec *specs, uint32_t oid);
bool vacuum_send_stop(CopyDataSpec *specs);

/* sentinel.c */
bool sentinel_setup(DatabaseCatalog *catalog,
					uint64_t startpos, uint64_t endpos);

bool sentinel_update_startpos(DatabaseCatalog *catalog, uint64_t startpos);
bool sentinel_update_endpos(DatabaseCatalog *catalog, uint64_t endpos);
bool sentinel_update_apply(DatabaseCatalog *catalog, bool apply);

bool sentinel_update_write_flush_lsn(DatabaseCatalog *catalog,
									 uint64_t write_lsn,
									 uint64_t flush_lsn);

bool sentinel_update_replay_lsn(DatabaseCatalog *catalog, uint64_t replay_lsn);

bool sentinel_get(DatabaseCatalog *catalog, CopyDBSentinel *sentinel);
bool sentinel_fetch(SQLiteQuery *query);

bool sentinel_sync_recv(DatabaseCatalog *catalog,
						uint64_t write_lsn,
						uint64_t flush_lsn,
						CopyDBSentinel *sentinel);

bool sentinel_sync_apply(DatabaseCatalog *catalog,
						 uint64_t replay_lsn,
						 CopyDBSentinel *sentinel);


/* summary.c */
bool print_summary(CopyDataSpec *specs);
bool summary_prepare_toplevel_durations(CopyDataSpec *specs);
bool prepare_summary_table(Summary *summary, CopyDataSpec *specs);

bool summary_oid_done_fetch(SQLiteQuery *query);

/*
 * Summary Table
 */
bool summary_lookup_table(DatabaseCatalog *catalog,
						  CopyTableDataSpec *tableSpecs);

bool summary_table_fetch(SQLiteQuery *query);

bool summary_add_table(DatabaseCatalog *catalog,
					   CopyTableDataSpec *tableSpecs);

bool summary_finish_table(DatabaseCatalog *catalog,
						  CopyTableDataSpec *tableSpecs);

bool summary_delete_table(DatabaseCatalog *catalog,
						  CopyTableDataSpec *tableSpecs);

bool summary_table_count_parts_done(DatabaseCatalog *catalog,
									CopyTableDataSpec *tableSpecs);

bool summary_table_fetch_count_parts_done(SQLiteQuery *query);

bool summary_add_table_parts_done(DatabaseCatalog *catalog,
								  CopyTableDataSpec *tableSpecs);

bool summary_lookup_table_parts_done(DatabaseCatalog *catalog,
									 CopyTableDataSpec *tableSpecs);

bool summary_table_parts_done_fetch(SQLiteQuery *query);

bool summary_add_vacuum(DatabaseCatalog *catalog,
						CopyTableDataSpec *tableSpecs);

bool summary_finish_vacuum(DatabaseCatalog *catalog,
						   CopyTableDataSpec *tableSpecs);

/*
 * Summary for Create Index and Constraints
 */
bool summary_lookup_index(DatabaseCatalog *catalog,
						  CopyIndexSpec *indexSpecs);

bool summary_index_fetch(SQLiteQuery *query);

bool summary_add_index(DatabaseCatalog *catalog,
					   CopyIndexSpec *indexSpecs);

bool summary_finish_index(DatabaseCatalog *catalog,
						  CopyIndexSpec *indexSpecs);

bool summary_delete_index(DatabaseCatalog *catalog,
						  CopyIndexSpec *indexSpecs);

bool summary_lookup_constraint(DatabaseCatalog *catalog,
							   CopyIndexSpec *indexSpecs);

bool summary_add_constraint(DatabaseCatalog *catalog,
							CopyIndexSpec *indexSpecs);

bool summary_finish_constraint(DatabaseCatalog *catalog,
							   CopyIndexSpec *indexSpecs);

bool summary_table_count_indexes_left(DatabaseCatalog *catalog,
									  CopyTableDataSpec *tableSpecs);

bool summary_table_fetch_count_indexes_left(SQLiteQuery *query);

bool summary_add_table_indexes_done(DatabaseCatalog *catalog,
									CopyTableDataSpec *tableSpecs);

bool summary_lookup_table_indexes_done(DatabaseCatalog *catalog,
									   CopyTableDataSpec *tableSpecs);

bool summary_table_indexes_done_fetch(SQLiteQuery *query);

bool summary_prepare_index_entry(DatabaseCatalog *catalog,
								 SourceIndex *index,
								 bool constraint,
								 SummaryIndexEntry *indexEntry);

/* compare.c */
bool compare_schemas(CopyDataSpec *copySpecs);
bool compare_data(CopyDataSpec *copySpecs);

bool compare_start_workers(CopyDataSpec *copySpecs, Queue *queue);
bool compare_queue_tables(CopyDataSpec *copySpecs, Queue *queue);
bool compare_data_worker(CopyDataSpec *copySpecs, Queue *queue);
bool compare_data_by_table_oid(CopyDataSpec *copySpecs, uint32_t oid);

bool compare_read_tables_sums(CopyDataSpec *copySpecs);
bool compare_table(CopyDataSpec *copySpecs, SourceTable *source);

bool compare_fetch_schemas(CopyDataSpec *copySpecs,
						   CopyDataSpec *sourceSpecs,
						   CopyDataSpec *targetSpecs);

bool compare_write_checksum(SourceTable *table, const char *filename);
bool compare_read_checksum(SourceTable *table, const char *filename);

#endif  /* COPYDB_H */
