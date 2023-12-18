/*
 * src/bin/pgcopydb/catalog.h
 *	 Catalog management as a SQLite internal file
 */

#ifndef CATALOG_H
#define CATALOG_H

#include "copydb.h"
#include "schema.h"

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
	CatalogFetchResult *fetchFunction;
	void *context;
};


/*
 * Catalog API.
 */
bool catalog_init_from_specs(CopyDataSpec *copySpecs);
bool catalog_close_from_specs(CopyDataSpec *copySpecs);

bool catalog_open(DatabaseCatalog *catalog);
bool catalog_init(DatabaseCatalog *catalog);
bool catalog_attach(DatabaseCatalog *a, DatabaseCatalog *b, const char *name);
bool catalog_close(DatabaseCatalog *catalog);

bool catalog_create_schema(DatabaseCatalog *catalog);
bool catalog_drop_schema(DatabaseCatalog *catalog);


bool catalog_begin(DatabaseCatalog *catalog);
bool catalog_commit(DatabaseCatalog *catalog);

bool catalog_register_setup(DatabaseCatalog *catalog,
							const char *source_pg_uri,
							const char *target_pg_uri,
							const char *snapshot,
							uint64_t splitTablesLargerThanBytes,
							const char *filters);

bool catalog_setup(DatabaseCatalog *catalog);
bool catalog_setup_fetch(SQLiteQuery *query);

bool catalog_register_section(DatabaseCatalog *catalog, CopyDataSection section);
bool catalog_section_state(DatabaseCatalog *catalog, CatalogSection *section);
bool catalog_section_fetch(SQLiteQuery *query);

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
 * Tables and their attributes and parts (COPY partitioning).
 */
bool catalog_add_s_table(DatabaseCatalog *catalog, SourceTable *table);
bool catalog_add_attributes(DatabaseCatalog *catalog, SourceTable *table);
bool catalog_add_s_table_part(DatabaseCatalog *catalog, SourceTable *table);
bool catalog_add_s_table_parts(DatabaseCatalog *catalog, SourceTable *table);

bool catalog_add_s_table_chksum(DatabaseCatalog *catalog,
								SourceTable *table,
								TableChecksum *srcChk,
								TableChecksum *dstChk);

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

typedef struct SourceTableIterator
{
	DatabaseCatalog *catalog;
	SourceTable *table;
	SQLiteQuery query;

	/* optional parameters */
	uint64_t splitTableLargerThanBytes;
} SourceTableIterator;

bool catalog_iter_s_table_init(SourceTableIterator *iter);
bool catalog_iter_s_table_nopk_init(SourceTableIterator *iter);
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

bool catalog_s_table_part_fetch(SQLiteQuery *query);

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
bool catalog_prepare_filter(DatabaseCatalog *catalog);

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

bool catalog_lookup_s_namespace_by_rlname(DatabaseCatalog *catalog,
										  const char *restoreListName,
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
 * Processes
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
