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

#include "cli_common.h"
#include "cli_root.h"
#include "pgsql.h"


typedef enum
{
	SOURCE_FILTER_EXCLUDE_SCHEMA = 0,
	SOURCE_FILTER_EXCLUDE_TABLE,
	SOURCE_FILTER_EXCLUDE_TABLE_DATA,
	SOURCE_FILTER_EXCLUDE_INDEX,
	SOURCE_FILTER_INCLUDE_ONLY_TABLE
} SourceFilterSection;

typedef struct SourceFilterSchema
{
	char nspname[NAMEDATALEN];
} SourceFilterSchema;

typedef struct SourceFilterSchemaList
{
	int count;
	SourceFilterSchema *array;  /* malloc'ed area */
} SourceFilterSchemaList;


typedef struct SourceFilterTable
{
	char nspname[NAMEDATALEN];
	char relname[NAMEDATALEN];
} SourceFilterTable;

typedef struct SourceFilterTableList
{
	int count;
	SourceFilterTable *array;   /* malloc'ed area */
} SourceFilterTableList;


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
	SourceFilterType type;
	SourceFilterSchemaList excludeSchemaList;
	SourceFilterTableList includeOnlyTableList;
	SourceFilterTableList excludeTableList;
	SourceFilterTableList excludeTableDataList;
	SourceFilterTableList excludeIndexList;
} SourceFilters;

char * filterTypeToString(SourceFilterType type);
SourceFilterType filterTypeComplement(SourceFilterType type);
bool parse_filters(const char *filebname, SourceFilters *filters);

#endif  /* FILTERING_H */
