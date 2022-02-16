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

typedef struct SourceFilters
{
	SourceFilterSchemaList excludeSchemaList;
	SourceFilterTableList includeOnlyTableList;
	SourceFilterTableList excludeTableList;
	SourceFilterTableList excludeTableDataList;
	SourceFilterTableList excludeIndexList;
} SourceFilters;


bool parse_filters(const char *filebname, SourceFilters *filters);

#endif  /* FILTERING_H */
