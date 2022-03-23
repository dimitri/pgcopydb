/*
 * src/bin/pgcopydb/filtering.c
 *     Implementation of a CLI which lets you run individual routines
 *     directly
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>

#include "env_utils.h"
#include "ini.h"
#include "log.h"
#include "filtering.h"
#include "parsing.h"
#include "string_utils.h"


static bool parse_filter_quoted_table_name(SourceFilterTable *table,
										   const char *qname);


/*
 * filterTypeComplement returns the complement to the given filtering type:
 * instead of listing the include-only tables, list the tables that are not
 * included; instead of listing tables that are not excluded, list the tables
 * that are excluded.
 */
SourceFilterType
filterTypeComplement(SourceFilterType type)
{
	switch (type)
	{
		case SOURCE_FILTER_TYPE_INCL:
		{
			return SOURCE_FILTER_TYPE_LIST_NOT_INCL;
		}

		case SOURCE_FILTER_TYPE_LIST_NOT_INCL:
		{
			return SOURCE_FILTER_TYPE_INCL;
		}

		case SOURCE_FILTER_TYPE_EXCL:
		{
			return SOURCE_FILTER_TYPE_LIST_EXCL;
		}

		case SOURCE_FILTER_TYPE_LIST_EXCL:
		{
			return SOURCE_FILTER_TYPE_EXCL;
		}

		case SOURCE_FILTER_TYPE_EXCL_INDEX:
		{
			return SOURCE_FILTER_TYPE_LIST_EXCL_INDEX;
		}

		case SOURCE_FILTER_TYPE_LIST_EXCL_INDEX:
		{
			return SOURCE_FILTER_TYPE_EXCL_INDEX;
		}

		default:
		{
			return SOURCE_FILTER_TYPE_NONE;
		}
	}
}


/*
 * parse_filters
 */
bool
parse_filters(const char *filename, SourceFilters *filters)
{
	char *fileContents = NULL;
	long fileSize = 0L;

	/* read the current postgresql.conf contents */
	if (!read_file(filename, &fileContents, &fileSize))
	{
		return false;
	}

	ini_t *ini = ini_load(fileContents, NULL);
	free(fileContents);

	/*
	 * The index in the sections array matches the SourceFilterSection enum
	 * values.
	 */
	struct section
	{
		char name[NAMEDATALEN];
		SourceFilterTableList *list;
	};

	struct section sections[] = {
		{ "exclude-schema", NULL },
		{ "exclude-table", &(filters->excludeTableList) },
		{ "exclude-table-data", &(filters->excludeTableDataList) },
		{ "exclude-index", &(filters->excludeIndexList) },
		{ "include-only-table", &(filters->includeOnlyTableList) },
		{ "", NULL },
	};

	for (int i = 0; sections[i].name[0] != '\0'; i++)
	{
		char *sectionName = sections[i].name;

		int sectionIndex = ini_find_section(ini, sectionName, 0);

		if (sectionIndex == INI_NOT_FOUND)
		{
			log_debug("Sections \"%s\" not found", sectionName);
			continue;
		}

		if (strcmp(ini_section_name(ini, sectionIndex), sectionName) != 0)
		{
			/* skip prefix match, only accept full lenght match */
			continue;
		}

		int optionCount = ini_property_count(ini, sectionIndex);

		log_debug("Section \"%s\" has %d entries", sections[i].name, optionCount);

		if (optionCount <= 0)
		{
			continue;
		}

		/*
		 * The index in the sections table is a SourceFilterSection enum value.
		 */
		switch (i)
		{
			case SOURCE_FILTER_EXCLUDE_SCHEMA:
			{
				filters->excludeSchemaList.count = optionCount;
				filters->excludeSchemaList.array = (SourceFilterSchema *)
												   malloc(optionCount *
														  sizeof(SourceFilterSchema));

				for (int o = 0; o < optionCount; o++)
				{
					SourceFilterSchema *schema =
						&(filters->excludeSchemaList.array[o]);

					const char *optionName =
						ini_property_name(ini, sectionIndex, o);

					strlcpy(schema->nspname, optionName, sizeof(schema->nspname));

					log_debug("excluding schema \"%s\"", schema->nspname);
				}
				break;
			}

			case SOURCE_FILTER_EXCLUDE_TABLE:
			case SOURCE_FILTER_EXCLUDE_TABLE_DATA:
			case SOURCE_FILTER_EXCLUDE_INDEX:
			case SOURCE_FILTER_INCLUDE_ONLY_TABLE:
			{
				SourceFilterTableList *list = sections[i].list;

				list->count = optionCount;
				list->array = (SourceFilterTable *)
							  malloc(optionCount * sizeof(SourceFilterTable));

				for (int o = 0; o < optionCount; o++)
				{
					SourceFilterTable *table = &(list->array[o]);

					const char *optionName =
						ini_property_name(ini, sectionIndex, o);

					if (!parse_filter_quoted_table_name(table, optionName))
					{
						/* errors have already been logged */
						(void) ini_destroy(ini);
						return false;
					}

					log_trace("%s \"%s\".\"%s\"",
							  sections[i].name,
							  table->nspname,
							  table->relname);
				}

				break;
			}

			default:
			{
				log_error("BUG: unknown section number %d", i);
				(void) ini_destroy(ini);
				return false;
			}
		}
	}

	(void) ini_destroy(ini);

	/*
	 * Now implement some checks: we can't implement both include-only-table
	 * and any other filtering rule, which are exclusion rules. Otherwise it's
	 * unclear what to do with tables that are not excluded and not included
	 * either.
	 */
	if (filters->includeOnlyTableList.count > 0 &&
		(filters->excludeTableList.count > 0 ||
		 filters->excludeSchemaList.count > 0))
	{
		log_error("Filtering setup in \"%s\" contains "
				  "%d entries in \"%s\" section and %d entries in \"%s\" "
				  "sections, please use only one of those.",
				  filename,
				  filters->includeOnlyTableList.count,
				  "include-only-table",
				  filters->excludeTableList.count,
				  "exclude-table");
		return false;
	}

	/*
	 * Now assign a proper type to the source filter.
	 */
	if (filters->includeOnlyTableList.count > 0)
	{
		filters->type = SOURCE_FILTER_TYPE_INCL;
	}
	else if (filters->excludeSchemaList.count > 0 ||
			 filters->excludeTableList.count > 0 ||
			 filters->excludeTableDataList.count > 0)
	{
		filters->type = SOURCE_FILTER_TYPE_EXCL;
	}
	else if (filters->excludeIndexList.count > 0)
	{
		/*
		 * If we reach this part of the code, it means we didn't include-only
		 * tables nor exclude any table (exclude-schema, exclude-table,
		 * exclude-table-data have not been used in the filtering setup), still
		 * the exclude-index clause has been used.
		 */
		filters->type = SOURCE_FILTER_TYPE_EXCL_INDEX;
	}
	else
	{
		filters->type = SOURCE_FILTER_TYPE_NONE;
	}

	return true;
}


/*
 * parse_filter_quoted_table_name parses a maybe-quoted qualified relation name
 * (schemaname.relname) into a pre-alllocated SourceFilterTable.
 */
static bool
parse_filter_quoted_table_name(SourceFilterTable *table, const char *qname)
{
	if (qname == NULL || qname[0] == '\0')
	{
		log_error("Failed to parse empty qualified name");
		return false;
	}

	char *dot = strchr(qname, '.');

	if (dot == NULL)
	{
		log_error("Failed to find a dot separator in qualified name \"%s\"",
				  qname);
		return false;
	}
	else if (dot == qname)
	{
		log_error("Failed to parse qualified name \"%s\": it starts with a dot",
				  qname);
		return false;
	}

	if (qname[0] == '"' && *(dot - 1) != '"')
	{
		char str[BUFSIZE] = { 0 };

		strlcpy(str, qname, Min(dot - qname, sizeof(str)));

		log_error("Failed to parse quoted relation name: %s", str);
		return false;
	}

	char *nspnameStart = qname[0] == '"' ? (char *) qname + 1 : (char *) qname;
	char *nspnameEnd = *(dot - 1) == '"' ? dot - 1 : dot;
	size_t nsplen = nspnameEnd - nspnameStart + 1;

	if (strlcpy(table->nspname, nspnameStart, nsplen) >= sizeof(table->nspname))
	{
		char str[BUFSIZE] = { 0 };
		strlcpy(str, nspnameStart, Min(nsplen, sizeof(str)));

		log_error("Failed to parse schema name \"%s\" (%lu bytes long), "
				  "pgcopydb and Postgres only support names up to %lu bytes",
				  str,
				  nsplen,
				  sizeof(table->nspname));
		return false;
	}

	if (strcmp(dot, ".") == 0)
	{
		log_error("Failed to parse empty relation name after the dot in \"%s\"",
				  qname);
		return false;
	}

	char *ptr = dot + 1;
	char *end = strchr(ptr, '\0');

	if (ptr[0] == '"' && *(end - 1) != '"')
	{
		char str[BUFSIZE] = { 0 };

		strlcpy(str, ptr, Min(end - ptr, sizeof(str)));

		log_error("Failed to parse quoted relation name: %s", str);
		return false;
	}

	char *relnameStart = ptr[0] == '"' ? ptr + 1 : ptr;
	char *relnameEnd = *(end - 1) == '"' ? end - 1 : end;
	size_t rellen = relnameEnd - relnameStart + 1;

	if (strlcpy(table->relname, relnameStart, rellen) >= sizeof(table->relname))
	{
		log_error("Failed to parse relation name \"%s\" (%lu bytes long), "
				  "pgcopydb and Postgres only support names up to %lu bytes",
				  ptr,
				  rellen,
				  sizeof(table->relname));
		return false;
	}

	return true;
}
