/*
 * src/bin/pgcopydb/schema.h
 *	 SQL queries to discover the source database schema
 */

#ifndef SCHEMA_H
#define SCHEMA_H

#include <stdbool.h>

#include "sqlite3.h"

#include "parson.h"
#include "uthash.h"

#include "filtering.h"
#include "lock_utils.h"
#include "pgsql.h"
#include "pg_utils.h"

/*
 * In the SQL standard we have "catalogs", which are then Postgres databases.
 * Much the same confusion as with namespace vs schema.
 */
typedef struct SourceDatabase
{
	uint32_t oid;
	char datname[PG_NAMEDATALEN];
	int64_t bytes;
	char bytesPretty[PG_NAMEDATALEN]; /* pg_size_pretty */
} SourceDatabase;


typedef struct SourceRole
{
	uint32_t oid;
	char rolname[PG_NAMEDATALEN];

	UT_hash_handle hh;          /* makes this structure hashable */
} SourceRole;


typedef struct SourceSchema
{
	uint32_t oid;
	char nspname[PG_NAMEDATALEN];
	char restoreListName[RESTORE_LIST_NAMEDATALEN];
} SourceSchema;


/*
 * SourceExtension caches the information we need about all the extensions
 * found in the source database.
 */
typedef struct SourceExtensionConfig
{
	uint32_t extoid;               /* extension's oid */
	uint32_t reloid;               /* pg_class.oid */
	char nspname[PG_NAMEDATALEN];
	char relname[PG_NAMEDATALEN];
	char *condition;            /* strdup from PQresult: malloc'ed area */
	char relkind;                  /* 'r' for regular table, 'S' for sequence */
} SourceExtensionConfig;


typedef struct SourceExtensionConfigArray
{
	int count;
	SourceExtensionConfig *array; /* malloc'ed area */
} SourceExtensionConfigArray;


typedef struct SourceExtension
{
	uint32_t oid;
	char extname[PG_NAMEDATALEN];
	char extnamespace[PG_NAMEDATALEN];
	bool extrelocatable;
	SourceExtensionConfigArray config;
} SourceExtension;


typedef struct ExtensionsVersions
{
	char name[PG_NAMEDATALEN];
	char defaultVersion[BUFSIZE];
	char installedVersion[BUFSIZE];
	JSON_Value *json;           /* malloc'ed area */
} ExtensionsVersions;

typedef struct ExtensionsVersionsArray
{
	int count;
	ExtensionsVersions *array;  /* malloc'ed area */
} ExtensionsVersionsArray;


typedef struct SourceCollation
{
	uint32_t oid;
	char collname[PG_NAMEDATALEN];
	char *desc;                 /* malloc'ed area */
	char restoreListName[RESTORE_LIST_NAMEDATALEN];
} SourceCollation;


/*
 * SourceTable caches the information we need about all the ordinary tables
 * found in the source database.
 */
typedef struct SourceTableParts
{
	int partNumber;
	int partCount;              /* zero when table is not partitionned */

	int64_t min;                /* WHERE partKey >= min */
	int64_t max;                /*   AND partKey  < max */

	int64_t count;              /* max - min + 1 */
} SourceTableParts;


typedef struct SourceTableAttribute
{
	int attnum;
	uint32_t atttypid;
	char attname[PG_NAMEDATALEN];
	bool attisprimary;
	bool attisgenerated;
} SourceTableAttribute;

typedef struct SourceTableAttributeArray
{
	int count;
	SourceTableAttribute *array; /* malloc'ed area */
} SourceTableAttributeArray;

/* forward declaration */
struct SourceIndexList;

/* checksum is formatted as uuid */
#define CHECKSUMLEN 36

typedef struct TableChecksum
{
	uint64_t rowcount;
	char checksum[CHECKSUMLEN];
} TableChecksum;

typedef struct SourceTable
{
	uint32_t oid;

	char qname[PG_NAMEDATALEN_FQ];
	char nspname[PG_NAMEDATALEN];
	char relname[PG_NAMEDATALEN];
	char amname[PG_NAMEDATALEN];
	char restoreListName[RESTORE_LIST_NAMEDATALEN];

	int64_t relpages;
	int64_t reltuples;
	int64_t bytes;
	int64_t partmin;
	int64_t partmax;

	char bytesPretty[PG_NAMEDATALEN]; /* pg_size_pretty */
	bool excludeData;

	TableChecksum sourceChecksum;
	TableChecksum targetChecksum;

	char partKey[PG_NAMEDATALEN];
	SourceTableParts partition;

	char *attrList;             /* malloc'ed area */
	SourceTableAttributeArray attributes;

	uint64_t indexCount;
	uint64_t constraintCount;

	/* summary information */
	uint64_t durationMs;
	uint64_t bytesTransmitted;
} SourceTable;


typedef struct SourceTableSize
{
	uint32_t oid;
	int64_t bytes;
	char bytesPretty[PG_NAMEDATALEN]; /* pg_size_pretty */
} SourceTableSize;

/* still used in progress.[ch] */
#define ARRAY_CAPACITY_INCREMENT 2

typedef struct SourceTableArray
{
	int count;
	int capacity;
	SourceTable *array;         /* malloc'ed area */
} SourceTableArray;


/*
 * SourceSequence caches the information we need about all the sequences found
 * in the source database.
 */
typedef struct SourceSequence
{
	uint32_t oid;
	uint32_t ownedby;           /* pg_class oid of OWNED BY table */
	uint32_t attrelid;          /* pg_class oid of table using as DEFAULT */
	uint32_t attroid;           /* pg_attrdef DEFAULT value OID */

	char qname[PG_NAMEDATALEN_FQ];
	char nspname[PG_NAMEDATALEN];
	char relname[PG_NAMEDATALEN];
	int64_t lastValue;
	bool isCalled;

	char restoreListName[RESTORE_LIST_NAMEDATALEN];
} SourceSequence;


/*
 * SourceIndex caches the information we need about all the indexes attached to
 * the ordinary tables found in the source database.
 */
typedef struct SourceIndex
{
	uint32_t indexOid;
	char indexQname[PG_NAMEDATALEN_FQ];
	char indexNamespace[PG_NAMEDATALEN];
	char indexRelname[PG_NAMEDATALEN];

	uint32_t tableOid;
	char tableQname[PG_NAMEDATALEN_FQ];
	char tableNamespace[PG_NAMEDATALEN];
	char tableRelname[PG_NAMEDATALEN];

	bool isPrimary;
	bool isUnique;
	char *indexColumns;         /* malloc'ed area */
	char *indexDef;             /* malloc'ed area */

	uint32_t constraintOid;
	bool condeferrable;
	bool condeferred;
	char constraintName[PG_NAMEDATALEN];
	char *constraintDef;        /* malloc'ed area */

	char indexRestoreListName[RESTORE_LIST_NAMEDATALEN];
	char constraintRestoreListName[RESTORE_LIST_NAMEDATALEN];
} SourceIndex;


/* still used in progress.[ch] */
typedef struct SourceIndexArray
{
	int count;
	int capacity;
	SourceIndex *array;         /* malloc'ed area */
} SourceIndexArray;


/*
 * SourceDepend caches the information about the dependency graph of
 * filtered-out objects. When filtering-out a table, we want to also filter-out
 * the foreign keys, views, materialized views and all that depend on this same
 * object.
 */
typedef struct SourceDepend
{
	char nspname[PG_NAMEDATALEN];
	char relname[PG_NAMEDATALEN];
	uint32_t refclassid;
	uint32_t refobjid;
	uint32_t classid;
	uint32_t objid;
	char deptype;
	char type[BUFSIZE];
	char identity[BUFSIZE];
} SourceDepend;


/*
 * SourceProperty caches data found in Postgres catalog pg_db_role_setting,
 * allowing to support ALTER DATABASE SET and ALTER ROLE IN DATABASE
 * properties.
 *
 * The setconfig format ("name=value") from the catalogs needs specific parsing
 * and re-writting in order to create the SQL statement needed to re-install
 * the properties, this is done when applying the properties, the same way as
 * pg_dump.
 */
typedef struct SourceProperty
{
	bool roleInDatabase;
	char rolname[PG_NAMEDATALEN];
	char datname[PG_NAMEDATALEN];
	char *setconfig;            /* malloc'ed area */
} SourceProperty;


/*
 * There is a cyclic dependency between schema.c and catalog.h, because the
 * schema queries need to fill-in the internal catalogs, and the internal
 * catalog API deals with schema.h structures (e.g. SourceTable or
 * SourceIndex).
 *
 * The easiest way to avoid the cyclic dependency issue at compile time seems
 * to be defining the top-level DatabaseCatalog structure in schema.h, that
 * needs to be #include'd in catalog.h anyway.
 */
typedef enum
{
	DATABASE_CATALOG_TYPE_UNKNOWN = 0,
	DATABASE_CATALOG_TYPE_SOURCE,
	DATABASE_CATALOG_TYPE_FILTER,
	DATABASE_CATALOG_TYPE_TARGET
} DatabaseCatalogType;


/*
 * Catalog setup and section allow decision-making about cache re-use and cache
 * invalidation.
 */
typedef struct CatalogSetup
{
	int id;                     /* 1 when setup has been done, otherwise zero */
	char *source_pguri;         /* malloc'ed area */
	char *target_pguri;         /* malloc'ed area */
	char snapshot[BUFSIZE];
	uint64_t splitTablesLargerThanBytes;
	char *filters;              /* malloc'ed area */
	char plugin[BUFSIZE];
	char slotName[BUFSIZE];
} CatalogSetup;

typedef enum
{
	DATA_SECTION_NONE = 0,
	DATA_SECTION_DATABASE_PROPERTIES,
	DATA_SECTION_COLLATIONS,
	DATA_SECTION_EXTENSIONS,
	DATA_SECTION_SCHEMA,
	DATA_SECTION_TABLE_DATA,
	DATA_SECTION_TABLE_DATA_PARTS,
	DATA_SECTION_SET_SEQUENCES,
	DATA_SECTION_INDEXES,
	DATA_SECTION_CONSTRAINTS,
	DATA_SECTION_DEPENDS,
	DATA_SECTION_FILTERS,
	DATA_SECTION_BLOBS,
	DATA_SECTION_VACUUM,
	DATA_SECTION_ALL
} CopyDataSection;

#define DATA_SECTION_COUNT (DATA_SECTION_ALL + 1)

typedef struct CatalogSection
{
	CopyDataSection section;
	char name[PG_NAMEDATALEN];
	bool fetched;
	uint64_t durationMs;
} CatalogSection;

typedef struct DatabaseCatalog
{
	DatabaseCatalogType type;

	CatalogSetup setup;
	CatalogSection sections[DATA_SECTION_COUNT];
	uint64_t totalDurationMs;

	char dbfile[MAXPGPATH];
	sqlite3 *db;

	Semaphore sema;
} DatabaseCatalog;


typedef struct Catalogs
{
	DatabaseCatalog source;
	DatabaseCatalog filter;
	DatabaseCatalog target;
} Catalogs;


bool schema_query_privileges(PGSQL *pgsql,
							 bool *hasDBCreatePrivilage,
							 bool *hasDBTempPrivilege);

bool schema_list_databases(PGSQL *pgsql, DatabaseCatalog *catalog);

bool schema_list_database_properties(PGSQL *pgsql, DatabaseCatalog *catalog);

bool schema_list_schemas(PGSQL *pgsql, DatabaseCatalog *catalog);

bool schema_list_roles(PGSQL *pgsql, DatabaseCatalog *catalog);

bool schema_list_ext_schemas(PGSQL *pgsql, DatabaseCatalog *catalog);

bool schema_list_extensions(PGSQL *pgsql, DatabaseCatalog *catalog);

bool schema_list_ext_versions(PGSQL *pgsql, ExtensionsVersionsArray *array);

bool schema_list_collations(PGSQL *pgsql, DatabaseCatalog *catalog);

bool schema_prepare_pgcopydb_table_size(PGSQL *pgsql,
										SourceFilters *filters, DatabaseCatalog *catalog);

bool schema_drop_pgcopydb_table_size(PGSQL *pgsql);

bool schema_list_ordinary_tables(PGSQL *pgsql,
								 SourceFilters *filters,
								 DatabaseCatalog *catalog);

bool schema_list_ordinary_tables_without_pk(PGSQL *pgsql,
											SourceFilters *filters,
											DatabaseCatalog *catalog);

bool schema_list_partitions(PGSQL *pgsql,
							DatabaseCatalog *catalog,
							SourceTable *table,
							uint64_t partSize);

bool schema_list_sequences(PGSQL *pgsql,
						   SourceFilters *filters,
						   DatabaseCatalog *catalog);

bool schema_get_sequence_value(PGSQL *pgsql, SourceSequence *seq);
bool schema_list_relpages(PGSQL *pgsql, SourceTable *table, DatabaseCatalog *catalog);
bool schema_set_sequence_value(PGSQL *pgsql, SourceSequence *seq);

bool schema_list_all_indexes(PGSQL *pgsql,
							 SourceFilters *filters,
							 DatabaseCatalog *catalog);

bool schema_list_table_indexes(PGSQL *pgsql,
							   const char *shemaName,
							   const char *tableName,
							   DatabaseCatalog *catalog);

bool schema_list_pg_depend(PGSQL *pgsql,
						   SourceFilters *filters,
						   DatabaseCatalog *catalog);

bool schema_send_table_checksum(PGSQL *pgsql, SourceTable *table);
bool schema_fetch_table_checksum(PGSQL *pgsql, TableChecksum *sum, bool *done);

#endif /* SCHEMA_H */
