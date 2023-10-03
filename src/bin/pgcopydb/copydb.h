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
	{ "statement_timeout", "0" }

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
		Queue *queue;
		Semaphore *semaphore;
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
	DATA_SECTION_EXTENSION,
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
 *
 * The OBJECT_KIND_DEFAULT goes with the pg_attribute catalog OID where
 * Postgres keeps a sequence default value call to nextval():
 *
 *   3291; 2604 497539 DEFAULT bar id dim
 */
typedef enum
{
	OBJECT_KIND_UNKNOWN = 0,
	OBJECT_KIND_SCHEMA,
	OBJECT_KIND_EXTENSION,
	OBJECT_KIND_COLLATION,
	OBJECT_KIND_TABLE,
	OBJECT_KIND_INDEX,
	OBJECT_KIND_CONSTRAINT,
	OBJECT_KIND_SEQUENCE,
	OBJECT_KIND_DEFAULT
} ObjectKind;


typedef struct SourceFilterItem
{
	uint32_t oid;

	ObjectKind kind;

	/* it's going to be only one of those, depending on the object kind */
	SourceSchema schema;
	SourceExtension extension;
	SourceCollation collation;
	SourceTable table;
	SourceSequence sequence;
	SourceIndex index;

	/* schema - name - owner */
	char restoreListName[RESTORE_LIST_NAMEDATALEN];

	UT_hash_handle hOid;            /* makes this structure hashable */
	UT_hash_handle hName;           /* makes this structure hashable */
} SourceFilterItem;


/*
 * Extensions versions to install on the target can be specified by the user.
 */
typedef struct ExtensionReqs
{
	char extname[PG_NAMEDATALEN];
	char version[BUFSIZE];

	UT_hash_handle hh;          /* makes this structure hashable */
} ExtensionReqs;


/* all that's needed to start a TABLE DATA copy for a whole database */
typedef struct CopyDataSpec
{
	CopyFilePaths cfPaths;
	PostgresPaths pgPaths;
	DirectoryState dirState;

	SourceFilters filters;
	SourceFilterItem *hOid;     /* hash table of objects, by Oid */
	SourceFilterItem *hName;    /* hash table of objects, by pg_restore name */

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
	bool noRolesPasswords;

	bool restart;
	bool resume;
	bool consistent;
	bool failFast;

	bool follow;                /* pgcopydb fork --follow */

	int tableJobs;
	int indexJobs;
	int vacuumJobs;
	int lObjectJobs;

	SplitTableLargerThan splitTablesLargerThan;

	Semaphore tableSemaphore;
	Semaphore indexSemaphore;

	Queue copyQueue;
	Queue indexQueue;
	Queue vacuumQueue;
	Queue loQueue;

	DumpPaths dumpPaths;

	/* results from calling has_database_privilege() on the source */
	bool hasDBCreatePrivilege;
	bool hasDBTempPrivilege;

	SourceCatalog catalog;
	TargetCatalog targetCatalog;
	CopyTableDataSpecsArray tableSpecsArray;
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

bool copydb_inspect_workdir(CopyFilePaths *cfPaths, DirectoryState *dirState);

bool copydb_rmdir_or_mkdir(const char *dir, bool removeDir);
bool copydb_prepare_dump_paths(CopyFilePaths *cfPaths, DumpPaths *dumpPaths);

bool copydb_init_specs(CopyDataSpec *specs,
					   CopyDBOptions *options,
					   CopyDataSection section);

bool copydb_init_table_specs(CopyTableDataSpec *tableSpecs,
							 CopyDataSpec *specs,
							 SourceTable *source,
							 int partNumber);

bool copydb_init_tablepaths(CopyFilePaths *cfPaths,
							TableFilePaths *tablePaths,
							uint32_t oid);

bool copydb_init_tablepaths_for_part(CopyFilePaths *cfPaths,
									 TableFilePaths *tablePaths,
									 uint32_t oid,
									 int partNumber);

bool copydb_export_snapshot(TransactionSnapshot *snapshot);

bool copydb_fatal_exit(void);
bool copydb_wait_for_subprocesses(bool failFast);

bool copydb_register_sysv_semaphore(SysVResArray *array, Semaphore *semaphore);
bool copydb_register_sysv_queue(SysVResArray *array, Queue *queue);

bool copydb_unlink_sysv_semaphore(SysVResArray *array, Semaphore *semaphore);
bool copydb_unlink_sysv_queue(SysVResArray *array, Queue *queue);

bool copydb_cleanup_sysv_resources(SysVResArray *array);


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
bool copydb_start_extension_data_process(CopyDataSpec *specs);
bool copydb_copy_extensions(CopyDataSpec *copySpecs, bool createExtensions);

bool copydb_parse_extensions_requirements(CopyDataSpec *copySpecs,
										  char *filename);

/* indexes.c */

bool copydb_start_index_workers(CopyDataSpec *specs);
bool copydb_index_worker(CopyDataSpec *specs);
bool copydb_create_index_by_oid(CopyDataSpec *specs, uint32_t indexOid);

bool copydb_add_table_indexes(CopyDataSpec *specs,
							  CopyTableDataSpec *tableSpecs);

bool copydb_index_workers_send_stop(CopyDataSpec *specs);

bool copydb_table_indexes_are_done(CopyDataSpec *specs,
								   SourceTable *table,
								   TableFilePaths *tablePaths,
								   bool *indexesAreDone,
								   bool *constraintsAreBeingBuilt);

bool copydb_init_index_paths(CopyFilePaths *cfPaths,
							 SourceIndex *index,
							 IndexFilePaths *indexPaths);

bool copydb_init_indexes_paths(CopyFilePaths *cfPaths,
							   SourceIndexArray *indexArray,
							   IndexFilePathsArray *indexPathsArray);

bool copydb_copy_all_indexes(CopyDataSpec *specs);

bool copydb_start_index_processes(CopyDataSpec *specs,
								  SourceIndexArray *indexArray,
								  IndexFilePathsArray *indexPathsArray);

bool copydb_start_index_process(CopyDataSpec *specs,
								SourceIndexArray *indexArray,
								IndexFilePathsArray *indexPathsArray);

bool copydb_create_index(const char *pguri,
						 SourceIndex *index,
						 IndexFilePaths *indexPaths,
						 Semaphore *lockFileSemaphore,
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
										 char **command);

bool copydb_prepare_create_constraint_command(SourceIndex *index,
											  char **command);

bool copydb_create_constraints(CopyDataSpec *spec, SourceTable *table);

/* dump_restore.c */
bool copydb_dump_source_schema(CopyDataSpec *specs,
							   const char *snapshot,
							   PostgresDumpSection section);
bool copydb_target_prepare_schema(CopyDataSpec *specs);
bool copydb_target_drop_tables(CopyDataSpec *specs);
bool copydb_target_finalize_schema(CopyDataSpec *specs);

bool copydb_objectid_has_been_processed_already(CopyDataSpec *specs,
												uint32_t oid);

bool copydb_write_restore_list(CopyDataSpec *specs, PostgresDumpSection section);

/* sequences.c */
bool copydb_copy_all_sequences(CopyDataSpec *specs);
bool copydb_start_seq_process(CopyDataSpec *specs);
bool copydb_prepare_sequence_specs(CopyDataSpec *specs, PGSQL *pgsql);

/* copydb_schema.c */
bool copydb_fetch_schema_and_prepare_specs(CopyDataSpec *specs);
bool copydb_objectid_is_filtered_out(CopyDataSpec *specs,
									 uint32_t oid,
									 char *restoreListName);

bool copydb_prepare_table_specs(CopyDataSpec *specs, PGSQL *pgsql);
bool copydb_prepare_index_specs(CopyDataSpec *specs, PGSQL *pgsql);
bool copydb_fetch_filtered_oids(CopyDataSpec *specs, PGSQL *pgsql);

char * copydb_ObjectKindToString(ObjectKind kind);

bool copydb_prepare_target_catalog(CopyDataSpec *specs);
bool copydb_schema_already_exists(CopyDataSpec *specs,
								  const char *restoreListName,
								  bool *exists);

/* table-data.c */
bool copydb_copy_all_table_data(CopyDataSpec *specs);
bool copydb_process_table_data(CopyDataSpec *specs);

bool copydb_start_copy_supervisor(CopyDataSpec *specs);
bool copydb_copy_supervisor(CopyDataSpec *specs);
bool copydb_start_table_data_workers(CopyDataSpec *specs);
bool copydb_table_data_worker(CopyDataSpec *specs);

bool copydb_add_copy(CopyDataSpec *specs, uint32_t oid, uint32_t part);
bool copydb_copy_data_by_oid(CopyDataSpec *specs, uint32_t oid, uint32_t part);

bool copydb_process_table_data_worker(CopyDataSpec *specs);

bool copydb_process_table_data_with_workers(CopyDataSpec *specs);

bool copydb_copy_table(CopyDataSpec *specs, CopyTableDataSpec *tableSpecs);


bool copydb_table_create_lockfile(CopyDataSpec *specs,
								  CopyTableDataSpec *tableSpecs,
								  bool *isDone);

bool copydb_mark_table_as_done(CopyDataSpec *specs,
							   CopyTableDataSpec *tableSpecs);

bool copydb_table_parts_are_all_done(CopyDataSpec *specs,
									 CopyTableDataSpec *tableSpecs,
									 bool *allPartsDone,
									 bool *isBeingProcessed);

bool copydb_prepare_copy_query(CopyTableDataSpec *tableSpecs,
							   PQExpBuffer query,
							   bool source);

/* blobs.c */
bool copydb_start_blob_process(CopyDataSpec *specs);

bool copydb_blob_supervisor(CopyDataSpec *specs);
bool copydb_start_blob_workers(CopyDataSpec *specs);
bool copydb_blob_worker(CopyDataSpec *specs);
bool copydb_queue_largeobject_metadata(CopyDataSpec *specs, uint64_t *count);
bool copydb_copy_blob_by_oid(CopyDataSpec *specs, uint32_t oid);
bool copydb_add_blob(CopyDataSpec *specs, uint32_t oid);
bool copydb_send_lo_stop(CopyDataSpec *specs);

/* vacuum.c */
bool vacuum_start_workers(CopyDataSpec *specs);
bool vacuum_worker(CopyDataSpec *specs);
bool vacuum_analyze_table_by_oid(CopyDataSpec *specs, uint32_t oid);
bool vacuum_add_table(CopyDataSpec *specs, uint32_t oid);
bool vacuum_send_stop(CopyDataSpec *specs);

/* summary.c */
bool prepare_summary_table(Summary *summary, CopyDataSpec *specs);
bool print_summary(Summary *summary, CopyDataSpec *specs);

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
