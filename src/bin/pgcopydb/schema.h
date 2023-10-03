/*
 * src/bin/pgcopydb/schema.h
 *	 SQL queries to discover the source database schema
 */

#ifndef SCHEMA_H
#define SCHEMA_H

#include <stdbool.h>

#include "parson.h"
#include "uthash.h"

#include "filtering.h"
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
}
SourceDatabase;

typedef struct SourceDatabaseArray
{
	int count;
	SourceDatabase *array;
} SourceDatabaseArray;


typedef struct SourceSchema
{
	uint32_t oid;
	char nspname[PG_NAMEDATALEN];
	char restoreListName[RESTORE_LIST_NAMEDATALEN];

	UT_hash_handle hh;          /* makes this structure hashable */
} SourceSchema;

typedef struct SourceSchemaArray
{
	int count;
	SourceSchema *array;        /* malloc'ed area */
} SourceSchemaArray;


/*
 * SourceExtension caches the information we need about all the extensions
 * found in the source database.
 */
typedef struct SourceExtensionConfig
{
	uint32_t oid;               /* pg_class.oid */
	char nspname[PG_NAMEDATALEN];
	char relname[PG_NAMEDATALEN];
	char *condition;            /* strdup from PQresult: malloc'ed area */
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


typedef struct SourceExtensionArray
{
	int count;
	SourceExtension *array;         /* malloc'ed area */
} SourceExtensionArray;


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

typedef struct SourceCollationArray
{
	int count;
	SourceCollation *array;         /* malloc'ed area */
} SourceCollationArray;


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


typedef struct SourceTablePartsArray
{
	int count;
	SourceTableParts *array;    /* malloc'ed area */
} SourceTablePartsArray;


typedef struct SourceTableAttribute
{
	int attnum;
	uint32_t atttypid;
	char attname[PG_NAMEDATALEN];
	bool attisprimary;
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

	int64_t relpages;
	int64_t reltuples;
	int64_t bytes;
	char bytesPretty[PG_NAMEDATALEN]; /* pg_size_pretty */
	bool excludeData;

	TableChecksum sourceChecksum;
	TableChecksum targetChecksum;

	char restoreListName[RESTORE_LIST_NAMEDATALEN];
	char partKey[PG_NAMEDATALEN];
	SourceTablePartsArray partsArray;

	SourceTableAttributeArray attributes;

	struct SourceIndexList *firstIndex;
	struct SourceIndexList *lastIndex;

	UT_hash_handle hh;          /* makes this structure hashable */
	UT_hash_handle hhQName;     /* makes this structure hashable */
} SourceTable;


typedef struct SourceTableArray
{
	int count;
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

	UT_hash_handle hh;          /* makes this structure hashable */
	UT_hash_handle hhQName;     /* makes this structure hashable */
} SourceSequence;


typedef struct SourceSequenceArray
{
	int count;
	SourceSequence *array;         /* malloc'ed area */
} SourceSequenceArray;

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

	UT_hash_handle hh;          /* makes this structure hashable */
} SourceIndex;


typedef struct SourceIndexArray
{
	int count;
	SourceIndex *array;         /* malloc'ed area */
} SourceIndexArray;


typedef struct SourceIndexList
{
	SourceIndex *index;
	struct SourceIndexList *next;
} SourceIndexList;


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


typedef struct SourceDependArray
{
	int count;
	SourceDepend *array;         /* malloc'ed area */
} SourceDependArray;

/*
 * SourceCatalog regroups all the information we fetch from a Postgres
 * instance.
 */
typedef struct SourceCatalog
{
	SourceExtensionArray extensionArray;
	SourceCollationArray collationArray;
	SourceTableArray sourceTableArray;
	SourceIndexArray sourceIndexArray;
	SourceSequenceArray sequenceArray;

	SourceTable *sourceTableHashByOid;
	SourceTable *sourceTableHashByQName;
	SourceIndex *sourceIndexHashByOid;
	SourceSequence *sourceSeqHashByOid;
	SourceSequence *sourceSeqHashByQname;
} SourceCatalog;


typedef struct TargetCatalog
{
	SourceSchemaArray schemaArray;
	SourceSchema *schemaHashByName;
} TargetCatalog;


bool schema_query_privileges(PGSQL *pgsql,
							 bool *hasDBCreatePrivilage,
							 bool *hasDBTempPrivilege);

bool schema_list_databases(PGSQL *pgsql, SourceDatabaseArray *catArray);

bool schema_list_schemas(PGSQL *pgsql, SourceSchemaArray *array);

bool schema_list_ext_schemas(PGSQL *pgsql, SourceSchemaArray *array);

bool schema_list_extensions(PGSQL *pgsql, SourceExtensionArray *extArray);

bool schema_list_ext_versions(PGSQL *pgsql, ExtensionsVersionsArray *array);

bool schema_list_collations(PGSQL *pgsql, SourceCollationArray *array);

bool schema_prepare_pgcopydb_table_size(PGSQL *pgsql,
										SourceFilters *filters,
										bool hasDBCreatePrivilege,
										bool cache,
										bool dropCache,
										bool *createdTableSizeTable);

bool schema_drop_pgcopydb_table_size(PGSQL *pgsql);

bool schema_list_ordinary_tables(PGSQL *pgsql,
								 SourceFilters *filters,
								 SourceTableArray *tableArray);

bool schema_list_ordinary_tables_without_pk(PGSQL *pgsql,
											SourceFilters *filters,
											SourceTableArray *tableArray);

bool schema_list_partitions(PGSQL *pgsql, SourceTable *table, uint64_t partSize);

bool schema_list_sequences(PGSQL *pgsql,
						   SourceFilters *filters,
						   SourceSequenceArray *seqArray);

bool schema_get_sequence_value(PGSQL *pgsql, SourceSequence *seq);
bool schema_set_sequence_value(PGSQL *pgsql, SourceSequence *seq);

bool schema_list_all_indexes(PGSQL *pgsql,
							 SourceFilters *filters,
							 SourceIndexArray *indexArray);

bool schema_list_table_indexes(PGSQL *pgsql,
							   const char *shemaName,
							   const char *tableName,
							   SourceIndexArray *indexArray);

bool schema_list_pg_depend(PGSQL *pgsql,
						   SourceFilters *filters,
						   SourceDependArray *dependArray);

bool schema_send_table_checksum(PGSQL *pgsql, SourceTable *table);
bool schema_fetch_table_checksum(PGSQL *pgsql, TableChecksum *sum, bool *done);

#endif /* SCHEMA_H */
