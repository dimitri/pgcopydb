/*
 * src/bin/pgcopydb/catalog.h
 *	 Catalog management as a SQLite internal file
 */

#ifndef CATALOG_H
#define CATALOG_H

#include "schema.h"
#include "string_utils.h"

/*
 * Internal infrastructure to bind values to SQLite prepared statements.
 */
typedef struct SQLiteQuery SQLiteQuery;
typedef bool (CatalogFetchResult)(SQLiteQuery *query);

struct SQLiteQuery
{
	sqlite3 *db;
	sqlite3_stmt *ppStmt;
	const char *sql;

	bool errorOnZeroRows;

	CatalogFetchResult *fetchFunction;
	void *context;
};


/*
 * Catalog API.
 */
bool catalog_open(DatabaseCatalog *catalog);
bool catalog_init(DatabaseCatalog *catalog);
bool catalog_create_semaphore(DatabaseCatalog *catalog);
bool catalog_attach(DatabaseCatalog *a, DatabaseCatalog *b, const char *name);
bool catalog_close(DatabaseCatalog *catalog);

bool catalog_create_schema(DatabaseCatalog *catalog);
bool catalog_drop_schema(DatabaseCatalog *catalog);

bool catalog_set_wal_mode(DatabaseCatalog *catalog);

bool catalog_begin(DatabaseCatalog *catalog, bool immediate);
bool catalog_commit(DatabaseCatalog *catalog);
bool catalog_rollback(DatabaseCatalog *catalog);

bool catalog_register_setup(DatabaseCatalog *catalog,
							const char *source_pg_uri,
							const char *target_pg_uri,
							const char *snapshot,
							uint64_t splitTablesLargerThanBytes,
							int splitMaxParts,
							const char *filters);

bool catalog_setup_replication(DatabaseCatalog *catalog,
							   const char *snapshot,
							   const char *plugin,
							   const char *slotName);

bool catalog_setup(DatabaseCatalog *catalog);
bool catalog_setup_fetch(SQLiteQuery *query);

/*
 * Catalog sections keep track of items that have been fetched to cache
 * already, such as tables, database properties, indexes, sequences, etc etc.
 * The sections are registered with a "fetched" boolean and some timing
 * information.
 *
 * To avoid a circular dependency between summary.h and catalog.h the timing
 * structures are defined here in catalog.h, and thus made available to
 * summary.h too.
 */
typedef enum
{
	TIMING_SECTION_UNKNOWN = 0,
	TIMING_SECTION_CATALOG_QUERIES,
	TIMING_SECTION_DUMP_SCHEMA,
	TIMING_SECTION_PREPARE_SCHEMA,
	TIMING_SECTION_TOTAL_DATA,
	TIMING_SECTION_COPY_DATA,
	TIMING_SECTION_CREATE_INDEX,
	TIMING_SECTION_ALTER_TABLE,
	TIMING_SECTION_VACUUM,
	TIMING_SECTION_SET_SEQUENCES,
	TIMING_SECTION_LARGE_OBJECTS,
	TIMING_SECTION_FINALIZE_SCHEMA,
	TIMING_SECTION_TOTAL
} TimingSection;

#define TIMING_SINGLE_JOB 1
#define TIMING_TABLE_JOBS 2
#define TIMING_INDEX_JOBS 4
#define TIMING_VACUUM_JOBS 8
#define TIMING_RESTORE_JOBS 16
#define TIMING_LOBJECTS_JOBS 32
#define TIMING_ALL_JOBS 64

typedef struct TopLevelTiming
{
	TimingSection section;
	bool cumulative;
	uint64_t startTime;         /* time(NULL) at start time */
	uint64_t doneTime;          /* time(NULL) at done time */
	uint64_t durationMs;        /* instr_time duration in milliseconds */
	instr_time startTimeInstr;  /* internal instr_time tracker */
	instr_time durationInstr;   /* internal instr_time tracker */
	char ppDuration[INTSTRING_MAX_DIGITS];
	uint32_t jobsMask;
	uint64_t count;             /* count objects or "things" */
	uint64_t bytes;             /* when relevant, sum bytes */
	char ppBytes[BUFSIZE];
	char *label;            /* malloc'ed area */
	const char *conn;
} TopLevelTiming;

void catalog_start_timing(TopLevelTiming *timing);
void catalog_stop_timing(TopLevelTiming *timing);

bool catalog_register_section(DatabaseCatalog *catalog, TopLevelTiming *timing);

bool catalog_section_state(DatabaseCatalog *catalog, CatalogSection *section);
bool catalog_section_fetch(SQLiteQuery *query);
bool catalog_total_duration(DatabaseCatalog *catalog);

bool catalog_extension_exists(DatabaseCatalog *catalog,
							const char *extensionName,
							bool *exists);
bool catalog_extension_fetch(SQLiteQuery *query);

char * CopyDataSectionToString(CopyDataSection section);

/*
 * Statistics over our catalogs.
 */
typedef struct CatalogTableStats
{
	uint64_t count;
	uint64_t countSplits;
	uint64_t countParts;
	uint64_t totalBytes;
	uint64_t totalTuples;
	char bytesPretty[BUFSIZE];
	char relTuplesPretty[BUFSIZE];
} CatalogTableStats;

typedef struct CatalogCounts
{
	uint64_t tables;
	uint64_t indexes;
	uint64_t constraints;
	uint64_t sequences;

	uint64_t roles;
	uint64_t databases;
	uint64_t namespaces;
	uint64_t extensions;
	uint64_t colls;
	uint64_t depends;
} CatalogCounts;


typedef struct CatalogStats
{
	CatalogTableStats table;
	CatalogCounts count;
} CatalogStats;

bool catalog_stats(DatabaseCatalog *catalog, CatalogStats *stats);
bool catalog_s_table_stats(DatabaseCatalog *catalog, CatalogTableStats *stats);
bool catalog_s_table_stats_fetch(SQLiteQuery *query);
bool catalog_count_objects(DatabaseCatalog *catalog, CatalogCounts *count);
bool catalog_count_fetch(SQLiteQuery *query);

/*
 * Materialized views
 */
typedef struct CatalogMatView
{
	uint32_t oid;
	char nspname[PG_NAMEDATALEN];
	char relname[PG_NAMEDATALEN];
	char restoreListName[RESTORE_LIST_NAMEDATALEN];
	bool excludeData;
} CatalogMatView;

bool catalog_add_s_matview(DatabaseCatalog *catalog, SourceTable *table);

bool catalog_lookup_s_matview_by_oid(DatabaseCatalog *catalog,
									 CatalogMatView *result,
									 uint32_t oid);
bool catalog_s_matview_fetch(SQLiteQuery *query);

/*
 * Tables and their attributes and parts (COPY partitioning).
 */
bool catalog_add_s_table(DatabaseCatalog *catalog, SourceTable *table);
bool catalog_add_attributes(DatabaseCatalog *catalog, SourceTable *table);
bool catalog_add_s_table_part(DatabaseCatalog *catalog, SourceTable *table);

bool catalog_add_s_table_chksum(DatabaseCatalog *catalog,
								SourceTable *table,
								TableChecksum *srcChk,
								TableChecksum *dstChk);

bool catalog_add_s_table_size(DatabaseCatalog *catalog,
							  SourceTableSize *tableSize);
bool catalog_delete_s_table(DatabaseCatalog *catalog,
							const char *nspname,
							const char *relname);

bool catalog_delete_s_table_chksum_all(DatabaseCatalog *catalog);

/*
 * To loop over our catalog "arrays" we provide an iterator based API, which
 * allows for allocating a single item in memory for the whole scan.
 */
typedef bool (SourceTableIterFun)(void *context, SourceTable *table);

bool catalog_iter_s_table(DatabaseCatalog *catalog,
						  void *context,
						  SourceTableIterFun *callback);

bool catalog_iter_s_table_nopk(DatabaseCatalog *catalog,
							   void *context,
							   SourceTableIterFun *callback);

bool catalog_iter_s_table_generated_columns(DatabaseCatalog *catalog,
											void *context,
											SourceTableIterFun *callback);

typedef struct SourceTableIterator
{
	DatabaseCatalog *catalog;
	SourceTable *table;
	SQLiteQuery query;
} SourceTableIterator;

bool catalog_iter_s_table_init(SourceTableIterator *iter);
bool catalog_iter_s_table_nopk_init(SourceTableIterator *iter);
bool catalog_iter_s_table_generated_columns_init(SourceTableIterator *iter);
bool catalog_iter_s_table_next(SourceTableIterator *iter);
bool catalog_iter_s_table_finish(SourceTableIterator *iter);

bool catalog_lookup_s_table(DatabaseCatalog *catalog,
							uint32_t oid,
							int partNumber,
							SourceTable *table);

bool catalog_lookup_s_table_by_name(DatabaseCatalog *catalog,
									const char *nspname,
									const char *relname,
									SourceTable *table);

bool catalog_s_table_fetch(SQLiteQuery *query);


typedef bool (SourceTablePartsIterFun)(void *context, SourceTableParts *part);

bool catalog_iter_s_table_parts(DatabaseCatalog *catalog,
								uint32_t oid,
								void *context,
								SourceTablePartsIterFun *callback);

typedef struct SourceTablePartsIterator
{
	DatabaseCatalog *catalog;
	SourceTableParts *part;
	SQLiteQuery query;

	/* optional parameters */
	uint32_t oid;
} SourceTablePartsIterator;

bool catalog_iter_s_table_part_init(SourceTablePartsIterator *iter);
bool catalog_iter_s_table_part_next(SourceTablePartsIterator *iter);
bool catalog_iter_s_table_part_finish(SourceTablePartsIterator *iter);

bool catalog_s_table_attrlist(DatabaseCatalog *catalog, SourceTable *table);
bool catalog_s_table_part_fetch(SQLiteQuery *query);

bool catalog_s_table_fetch_attrlist(SQLiteQuery *query);

typedef struct SourceTableAttrsIterator
{
	DatabaseCatalog *catalog;
	SourceTable *table;
	SQLiteQuery query;
	bool done;
} SourceTableAttrsIterator;

bool catalog_s_table_fetch_attrs(DatabaseCatalog *catalog,
								 SourceTable *table);

bool catalog_iter_s_table_attrs_init(SourceTableAttrsIterator *iter);
bool catalog_iter_s_table_attrs_next(SourceTableAttrsIterator *iter);
bool catalog_iter_s_table_attrs_finish(SourceTableAttrsIterator *iter);

bool catalog_s_table_attrs_fetch(SQLiteQuery *query);

bool catalog_s_table_count_attrs(DatabaseCatalog *catalog,
								 SourceTable *table);

bool catalog_s_table_count_attrs_fetch(SQLiteQuery *query);


bool catalog_lookup_s_attr_by_name(DatabaseCatalog *catalog,
								   uint32_t reloid,
								   const char *attname,
								   SourceTableAttribute *attribute);

bool catalog_s_attr_fetch(SQLiteQuery *query);

/*
 * Indexes
 */
bool catalog_add_s_index(DatabaseCatalog *catalog, SourceIndex *index);
bool catalog_add_s_constraint(DatabaseCatalog *catalog, SourceIndex *index);

bool catalog_lookup_s_index(DatabaseCatalog *catalog,
							uint32_t oid,
							SourceIndex *index);

bool catalog_lookup_s_index_by_name(DatabaseCatalog *catalog,
									const char *nspname,
									const char *relname,
									SourceIndex *index);

bool catalog_delete_s_index_table(DatabaseCatalog *catalog,
								  const char *nspname,
								  const char *relname);

bool catalog_delete_s_index_all(DatabaseCatalog *catalog);


typedef bool (SourceIndexIterFun)(void *context, SourceIndex *index);

bool catalog_iter_s_index(DatabaseCatalog *catalog,
						  void *context,
						  SourceIndexIterFun *callback);

bool catalog_iter_s_index_table(DatabaseCatalog *catalog,
								const char *nspname,
								const char *relname,
								void *context,
								SourceIndexIterFun *callback);

bool catalog_s_table_count_indexes(DatabaseCatalog *catalog,
								   SourceTable *table);

bool catalog_s_table_count_indexes_fetch(SQLiteQuery *query);

typedef struct SourceIndexIterator
{
	DatabaseCatalog *catalog;
	SourceIndex *index;
	SQLiteQuery query;

	/* optional parameters */
	const char *nspname;
	const char *relname;
} SourceIndexIterator;

bool catalog_iter_s_index_init(SourceIndexIterator *iter);
bool catalog_iter_s_index_table_init(SourceIndexIterator *iter);
bool catalog_iter_s_index_next(SourceIndexIterator *iter);
bool catalog_iter_s_index_finish(SourceIndexIterator *iter);

bool catalog_s_index_fetch(SQLiteQuery *query);

/*
 * Sequences
 */
bool catalog_add_s_seq(DatabaseCatalog *catalog, SourceSequence *index);
bool catalog_update_sequence_values(DatabaseCatalog *catalog, SourceSequence *seq);
bool catalog_update_s_table_relpages(DatabaseCatalog *catalog, SourceTable *sourceTable);

typedef bool (SourceSequenceIterFun)(void *context, SourceSequence *seq);

bool catalog_iter_s_seq(DatabaseCatalog *catalog,
						void *context,
						SourceSequenceIterFun *callback);

bool catalog_lookup_s_seq_by_name(DatabaseCatalog *catalog,
								  const char *nspname,
								  const char *relname,
								  SourceSequence *seq);

typedef struct SourceSeqIterator
{
	DatabaseCatalog *catalog;
	SourceSequence *seq;
	SQLiteQuery query;
} SourceSeqIterator;

bool catalog_iter_s_seq_init(SourceSeqIterator *iter);
bool catalog_iter_s_seq_next(SourceSeqIterator *iter);
bool catalog_iter_s_seq_finish(SourceSeqIterator *iter);

bool catalog_s_seq_fetch(SQLiteQuery *query);

/*
 * Filtering is done through a single table that concatenates the Oid and
 * pg_restore archives TOC list names (restore_list_name) in such a way that we
 * can get away with a single hash-table like lookup.
 */
bool catalog_prepare_filter(DatabaseCatalog *catalog,
							bool skipExtensions,
							bool skipCollations);

typedef struct CatalogFilter
{
	uint32_t oid;
	char restoreListName[RESTORE_LIST_NAMEDATALEN];
	char kind[PG_NAMEDATALEN];
} CatalogFilter;

bool catalog_lookup_filter_by_oid(DatabaseCatalog *catalog,
								  CatalogFilter *result,
								  uint32_t oid);

bool catalog_lookup_filter_by_rlname(DatabaseCatalog *catalog,
									 CatalogFilter *result,
									 const char *restoreListName);

bool catalog_filter_fetch(SQLiteQuery *query);

/*
 * Databases
 */
bool catalog_add_s_database(DatabaseCatalog *catalog, SourceDatabase *dat);

bool catalog_add_s_database_properties(DatabaseCatalog *catalog,
									   SourceProperty *guc);

typedef bool (SourceDatabaseIterFun)(void *context, SourceDatabase *dat);

bool catalog_iter_s_database(DatabaseCatalog *catalog,
							 void *context,
							 SourceDatabaseIterFun *callback);

typedef struct SourceDatabaseIterator
{
	DatabaseCatalog *catalog;
	SourceDatabase *dat;
	SQLiteQuery query;
} SourceDatabaseIterator;

bool catalog_iter_s_database_init(SourceDatabaseIterator *iter);
bool catalog_iter_s_database_next(SourceDatabaseIterator *iter);
bool catalog_iter_s_database_finish(SourceDatabaseIterator *iter);

bool catalog_s_database_fetch(SQLiteQuery *query);

typedef bool (SourcePropertyIterFun)(void *context, SourceProperty *property);

bool catalog_iter_s_database_guc(DatabaseCatalog *catalog,
								 const char *dbname,
								 void *context,
								 SourcePropertyIterFun *callback);

typedef struct SourcePropertyIterator
{
	DatabaseCatalog *catalog;
	SourceProperty *property;
	SQLiteQuery query;
	const char *dbname;
} SourcePropertyIterator;

bool catalog_iter_s_database_guc_init(SourcePropertyIterator *iter);
bool catalog_iter_s_database_guc_next(SourcePropertyIterator *iter);
bool catalog_iter_s_database_guc_finish(SourcePropertyIterator *iter);

bool catalog_s_database_guc_fetch(SQLiteQuery *query);


/*
 * Namespaces
 */
bool catalog_add_s_namespace(DatabaseCatalog * catalog, SourceSchema *namespace);
bool catalog_lookup_s_namespace_by_oid(DatabaseCatalog *catalog,
									   uint32_t oid,
									   SourceSchema *result);
bool catalog_lookup_s_namespace_by_nspname(DatabaseCatalog *catalog,
										   const char *nspname,
										   SourceSchema *result);

bool catalog_s_namespace_fetch(SQLiteQuery *query);

/*
 * Roles
 */
bool catalog_add_s_role(DatabaseCatalog *catalog, SourceRole *role);

bool catalog_lookup_s_role_by_name(DatabaseCatalog *catalog,
								   const char *rolname,
								   SourceRole *role);

bool catalog_s_role_fetch(SQLiteQuery *query);


/*
 * Extensions
 */
bool catalog_add_s_extension(DatabaseCatalog *catalog,
							 SourceExtension *extension);

bool catalog_add_s_extension_config(DatabaseCatalog *catalog,
									SourceExtensionConfig *config);

typedef bool (SourceExtensionIterFun)(void *context, SourceExtension *ext);

bool catalog_iter_s_extension(DatabaseCatalog *catalog,
							  void *context,
							  SourceExtensionIterFun *callback);

typedef struct SourceExtensionIterator
{
	DatabaseCatalog *catalog;
	SourceExtension *ext;
	SQLiteQuery query;
} SourceExtensionIterator;

bool catalog_iter_s_extension_init(SourceExtensionIterator *iter);
bool catalog_iter_s_extension_next(SourceExtensionIterator *iter);
bool catalog_s_extension_fetch(SQLiteQuery *query);
bool catalog_iter_s_extension_finish(SourceExtensionIterator *iter);

typedef struct SourceExtConfigIterator
{
	DatabaseCatalog *catalog;
	SourceExtension *ext;
	SQLiteQuery query;
	bool done;
} SourceExtConfigIterator;


bool catalog_s_ext_fetch_extconfig(DatabaseCatalog *catalog, SourceExtension *ext);
bool catalog_iter_s_ext_extconfig_init(SourceExtConfigIterator *iter);
bool catalog_iter_s_ext_extconfig_next(SourceExtConfigIterator *iter);
bool catalog_iter_s_ext_extconfig_finish(SourceExtConfigIterator *iter);
bool catalog_s_ext_extconfig_fetch(SQLiteQuery *query);


/*
 * Collations
 */
bool catalog_add_s_coll(DatabaseCatalog *catalog, SourceCollation *coll);

typedef bool (SourceCollationIterFun)(void *context, SourceCollation *coll);

bool catalog_iter_s_coll(DatabaseCatalog *catalog,
						 void *context,
						 SourceCollationIterFun *callback);

typedef struct SourceCollationIterator
{
	DatabaseCatalog *catalog;
	SourceCollation *coll;
	SQLiteQuery query;
} SourceCollationIterator;

bool catalog_iter_s_coll_init(SourceCollationIterator *iter);
bool catalog_iter_s_coll_next(SourceCollationIterator *iter);
bool catalog_iter_s_coll_finish(SourceCollationIterator *iter);

bool catalog_s_coll_fetch(SQLiteQuery *query);

/*
 * Dependencies
 */
bool catalog_add_s_depend(DatabaseCatalog *catalog, SourceDepend *depend);

typedef bool (SourceDependIterFun)(void *context, SourceDepend *coll);

bool catalog_iter_s_depend(DatabaseCatalog *catalog,
						   void *context,
						   SourceDependIterFun *callback);

typedef struct SourceDependIterator
{
	DatabaseCatalog *catalog;
	SourceDepend *dep;
	SQLiteQuery query;
} SourceDependIterator;

bool catalog_iter_s_depend_init(SourceDependIterator *iter);
bool catalog_iter_s_depend_next(SourceDependIterator *iter);
bool catalog_iter_s_depend_finish(SourceDependIterator *iter);

bool catalog_s_depend_fetch(SQLiteQuery *query);

/*
 * Processes, progress, summary
 */
typedef struct ProcessInfo
{
	pid_t pid;
	char psType[PG_NAMEDATALEN];
	char *psTitle;
	uint32_t tableOid;
	uint32_t partNumber;
	uint32_t indexOid;
} ProcessInfo;


bool catalog_upsert_process_info(DatabaseCatalog *catalog, ProcessInfo *ps);
bool catalog_delete_process(DatabaseCatalog *catalog, pid_t pid);

bool catalog_iter_s_table_in_copy(DatabaseCatalog *catalog,
								  void *context,
								  SourceTableIterFun *callback);

bool catalog_iter_s_table_in_copy_init(SourceTableIterator *iter);

bool catalog_iter_s_index_in_progress(DatabaseCatalog *catalog,
									  void *context,
									  SourceIndexIterFun *callback);

bool catalog_iter_s_index_in_progress_init(SourceIndexIterator *iter);


typedef struct CatalogProgressCount
{
	uint64_t table;
	uint64_t index;
} CatalogProgressCount;

bool catalog_count_summary_done(DatabaseCatalog *catalog,
								CatalogProgressCount *count);
bool catalog_count_summary_done_fetch(SQLiteQuery *query);


/*
 * Logical decoding
 */
bool catalog_add_timeline_history(DatabaseCatalog *catalog,
								  TimelineHistoryEntry *entry);
bool catalog_lookup_timeline_history(DatabaseCatalog *catalog,
									 int tli,
									 TimelineHistoryEntry *entry);
bool catalog_timeline_history_fetch(SQLiteQuery *query);

/*
 * Internal tooling for catalogs management
 */
typedef enum
{
	BIND_PARAMETER_TYPE_UNKNOWN = 0,
	BIND_PARAMETER_TYPE_INT,
	BIND_PARAMETER_TYPE_INT64,
	BIND_PARAMETER_TYPE_TEXT
} BindParameterType;


typedef struct BindParam
{
	BindParameterType type;
	char *name;
	uint64_t intVal;
	char *strVal;
} BindParam;


bool catalog_execute(DatabaseCatalog *catalog, char *sql);

bool catalog_sql_prepare(sqlite3 *db, const char *sql, SQLiteQuery *query);
bool catalog_sql_bind(SQLiteQuery *query, BindParam *params, int count);
bool catalog_sql_execute(SQLiteQuery *query);
bool catalog_sql_execute_once(SQLiteQuery *query);
bool catalog_sql_finalize(SQLiteQuery *query);

int catalog_sql_step(SQLiteQuery *query);

bool catalog_bind_parameters(sqlite3 *db,
							 sqlite3_stmt *ppStmt,
							 BindParam *params,
							 int count);

#endif  /* CATALOG_H */
