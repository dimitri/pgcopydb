/*
 * src/bin/pgcopydb/filtering.h
 *     Implementation of a CLI which lets you run individual routines
 *     directly
 */

#ifndef FILTERING_H
#define FILTERING_H

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>

#include "pgsql.h"


typedef enum
{
	SOURCE_FILTER_UNKNOWN = 0,
	SOURCE_FILTER_INCLUDE_ONLY_SCHEMA,
	SOURCE_FILTER_EXCLUDE_SCHEMA,
	SOURCE_FILTER_EXCLUDE_TABLE,
	SOURCE_FILTER_EXCLUDE_TABLE_DATA,
	SOURCE_FILTER_EXCLUDE_INDEX,
	SOURCE_FILTER_INCLUDE_ONLY_TABLE,
	SOURCE_FILTER_EXCLUDE_EXTENSION,
	SOURCE_FILTER_INCLUDE_ONLY_EXTENSION
} SourceFilterSection;

typedef struct SourceFilterSchema
{
	char nspname[PG_NAMEDATALEN];        /* bare name from pg_namespace (after normalization) */
	char restoreListName[PG_NAMEDATALEN]; /* quote_ident form for pg_dump/pg_restore args */
} SourceFilterSchema;

typedef struct SourceFilterSchemaList
{
	int count;
	int countOriginal;          /* count before pattern expansion (0 = no expansion) */
	SourceFilterSchema *array;  /* malloc'ed area */
} SourceFilterSchemaList;


typedef struct SourceFilterTable
{
	char nspname[PG_NAMEDATALEN];
	char relname[PG_NAMEDATALEN];
} SourceFilterTable;

typedef struct SourceFilterTableList
{
	int count;
	int countOriginal;          /* count before pattern expansion (0 = no expansion) */
	SourceFilterTable *array;   /* malloc'ed area */
} SourceFilterTableList;


typedef struct SourceFilterExtension
{
	char extname[PG_NAMEDATALEN];
} SourceFilterExtension;

typedef struct SourceFilterExtensionList
{
	int count;
	SourceFilterExtension *array;   /* malloc'ed area */
} SourceFilterExtensionList;


/*
 * SourceFilterSchemaPattern holds a single POSIX ERE pattern for a schema
 * name, parsed from an INI entry with the ~/pattern/ delimiter syntax.
 * Patterns are non-anchored by default: ~/staging_/ matches any schema whose
 * name contains "staging_".  Use ~/^staging_/ to anchor at the start.
 *
 * The regex is validated at parse time (compiled and immediately freed) but
 * the actual matching is done server-side via PostgreSQL's ~ operator in
 * filters_validate_and_normalize(), so no compiled regex_t is stored here.
 */
typedef struct SourceFilterSchemaPattern
{
	char nspname_re[BUFSIZE];   /* POSIX ERE pattern for the schema name */
} SourceFilterSchemaPattern;

typedef struct SourceFilterSchemaPatternList
{
	int count;
	SourceFilterSchemaPattern *array;   /* malloc'ed area */
} SourceFilterSchemaPatternList;


/*
 * SourceFilterTablePattern holds one row of the SQLite filter_*_pattern
 * tables: exactly one of (nspname, nspname_re) is non-empty and exactly
 * one of (relname, relname_re) is non-empty.
 *
 *   nspname[0]    != '\0' && nspname_re[0] == '\0'  -> exact schema match
 *   nspname[0]    == '\0' && nspname_re[0] != '\0'  -> regex schema match
 *   relname[0]    != '\0' && relname_re[0] == '\0'  -> exact table match
 *   relname_re[0] != '\0' && relname[0]   == '\0'   -> regex table match
 *
 * Regexes are validated at parse time but matching is done server-side via
 * PostgreSQL's ~ operator in filters_validate_and_normalize().
 */
typedef struct SourceFilterTablePattern
{
	char nspname[PG_NAMEDATALEN];   /* exact schema name, or empty */
	char nspname_re[BUFSIZE];        /* POSIX ERE for schema, or empty */
	char relname[PG_NAMEDATALEN];   /* exact table name, or empty */
	char relname_re[BUFSIZE];        /* POSIX ERE for table, or empty */
} SourceFilterTablePattern;

typedef struct SourceFilterTablePatternList
{
	int count;
	SourceFilterTablePattern *array;   /* malloc'ed area */
} SourceFilterTablePatternList;


/*
 * Define a Source Filter Type that allows producing the right kind of SQL
 * query. To that end, we need to distinguish if we're going to:
 *
 * - include only some tables (inner join)
 *
 * - exclude some tables (exclude-schema, exclude-table, exclude-table-data all
 *   lead to the same kind of anti-join form based on left join where
 *   right-side is null)
 *
 * - or exclude only some indexes (no filtering on schema queries for tables,
 *   only on the schema queries for indexes).
 *
 * Adding to that, we also need to produce a list of OIDs to skip in the
 * pg_dump catalog when calling into pg_restore. The include-only-table filter
 * is already implemented, see `copydb_objectid_has_been_processed_already'.
 * The exclusion filters need to be implemented as an inner join query if we
 * want to list the OIDs of skipped objects.
 *
 */
typedef enum
{
	SOURCE_FILTER_TYPE_NONE = 0,
	SOURCE_FILTER_TYPE_INCL,
	SOURCE_FILTER_TYPE_EXCL,

	SOURCE_FILTER_TYPE_LIST_NOT_INCL,
	SOURCE_FILTER_TYPE_LIST_EXCL,

	SOURCE_FILTER_TYPE_EXCL_INDEX,
	SOURCE_FILTER_TYPE_LIST_EXCL_INDEX
} SourceFilterType;

typedef struct SourceFilters
{
	bool prepared;
	bool normalized;
	SourceFilterType type;
	SourceFilterSchemaList includeOnlySchemaList;
	SourceFilterSchemaList excludeSchemaList;
	SourceFilterTableList includeOnlyTableList;
	SourceFilterTableList excludeTableList;
	SourceFilterTableList excludeTableDataList;
	SourceFilterTableList excludeIndexList;
	SourceFilterExtensionList excludeExtensionList;
	SourceFilterExtensionList includeOnlyExtensionList;

	/*
	 * Regex pattern lists, parsed from ~/pattern/ INI entries.
	 * filters_validate_and_normalize() expands each pattern by querying
	 * pg_catalog and appending exact matches to the corresponding list above.
	 * The pattern lists themselves are written to SQLite for debugging.
	 */
	SourceFilterSchemaPatternList includeOnlySchemaPatternList;
	SourceFilterSchemaPatternList excludeSchemaPatternList;
	SourceFilterTablePatternList includeOnlyTablePatternList;
	SourceFilterTablePatternList excludeTablePatternList;
	SourceFilterTablePatternList excludeTableDataPatternList;
	SourceFilterTablePatternList excludeIndexPatternList;
} SourceFilters;

char * filterTypeToString(SourceFilterType type);
SourceFilterType filterTypeComplement(SourceFilterType type);
bool parse_filters(const char *filebname, SourceFilters *filters);
bool filters_validate_and_normalize(PGSQL *pgsql, SourceFilters *filters);

bool filters_as_json(SourceFilters *filters, JSON_Value *jsFilter);

bool filter_entry_is_pattern(const char *entry);
bool parse_filter_table_pattern(SourceFilterTablePattern *pattern,
								const char *entry);
bool parse_filter_schema_pattern(SourceFilterSchemaPattern *pattern,
								 const char *entry);

#endif  /* FILTERING_H */
