/*
 * src/bin/pgcopydb/filtering.c
 *     Implementation of a CLI which lets you run individual routines
 *     directly
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>

#include "env_utils.h"
#include "file_utils.h"
#include "ini.h"
#include "log.h"
#include "filtering.h"
#include "parsing_utils.h"
#include "string_utils.h"


static bool parse_filter_quoted_table_name(SourceFilterTable *table,
										   const char *qname);


/*
 * filterTypeToString returns a string reprensentation of the enum value.
 */
char *
filterTypeToString(SourceFilterType type)
{
	switch (type)
	{
		case SOURCE_FILTER_TYPE_NONE:
		{
			return "SOURCE_FILTER_TYPE_NONE";
		}

		case SOURCE_FILTER_TYPE_INCL:
		{
			return "SOURCE_FILTER_TYPE_INCL";
		}

		case SOURCE_FILTER_TYPE_EXCL:
		{
			return "SOURCE_FILTER_TYPE_EXCL";
		}

		case SOURCE_FILTER_TYPE_LIST_NOT_INCL:
		{
			return "SOURCE_FILTER_TYPE_LIST_NOT_INCL";
		}

		case SOURCE_FILTER_TYPE_LIST_EXCL:
		{
			return "SOURCE_FILTER_LIST_EXCL";
		}

		case SOURCE_FILTER_TYPE_EXCL_INDEX:
		{
			return "SOURCE_FILTER_TYPE_EXCL_INDEX";
		}

		case SOURCE_FILTER_TYPE_LIST_EXCL_INDEX:
		{
			return "SOURCE_FILTER_TYPE_LIST_EXCL_INDEX";
		}
	}

	/* that's a bug, the lack of a default branch above should prevent it */
	return "SOURCE FILTER TYPE UNKNOWN";
}


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
		SourceFilterSection section;
		SourceFilterTableList *list;
	}
	sections[] =
	{
		{ "include-only-schema", SOURCE_FILTER_INCLUDE_ONLY_SCHEMA, NULL },
		{ "exclude-schema", SOURCE_FILTER_EXCLUDE_SCHEMA, NULL },
		{
			"exclude-table",
			SOURCE_FILTER_EXCLUDE_TABLE,
			&(filters->excludeTableList)
		},
		{
			"exclude-table-data",
			SOURCE_FILTER_EXCLUDE_TABLE_DATA,
			&(filters->excludeTableDataList)
		},
		{
			"exclude-index",
			SOURCE_FILTER_EXCLUDE_INDEX,
			&(filters->excludeIndexList)
		},
		{
			"include-only-table",
			SOURCE_FILTER_INCLUDE_ONLY_TABLE,
			&(filters->includeOnlyTableList)
		},
		{ "", SOURCE_FILTER_UNKNOWN, NULL },
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
		switch (sections[i].section)
		{
			case SOURCE_FILTER_INCLUDE_ONLY_SCHEMA:
			{
				filters->includeOnlySchemaList.count = optionCount;
				filters->includeOnlySchemaList.array =
					(SourceFilterSchema *) calloc(optionCount,
												  sizeof(SourceFilterSchema));

				for (int o = 0; o < optionCount; o++)
				{
					SourceFilterSchema *schema =
						&(filters->includeOnlySchemaList.array[o]);

					const char *optionName =
						ini_property_name(ini, sectionIndex, o);

					strlcpy(schema->nspname, optionName, sizeof(schema->nspname));

					log_debug("including only schema \"%s\"", schema->nspname);
				}
				break;
			}

			case SOURCE_FILTER_EXCLUDE_SCHEMA:
			{
				filters->excludeSchemaList.count = optionCount;
				filters->excludeSchemaList.array =
					(SourceFilterSchema *) calloc(optionCount,
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
	 *
	 * Using both exclude-schema and include-only-table sections is allowed,
	 * the user needs to pay attention not to exclude schemas of tables that
	 * are then to be included only.
	 *
	 * Using both exclude-schema and include-only-schema is disallowed too. It
	 * does not make sense to use both at the same time.
	 */
	if (filters->includeOnlySchemaList.count > 0 &&
		filters->excludeSchemaList.count > 0)
	{
		log_error("Filtering setup in \"%s\" contains %d entries "
				  "in section \"%s\" and %d entries in section \"%s\", "
				  "please use only one of these section.",
				  filename,
				  filters->includeOnlySchemaList.count,
				  "include-only-schema",
				  filters->excludeSchemaList.count,
				  "exclude-schema");
		return false;
	}

	if (filters->includeOnlyTableList.count > 0 &&
		filters->excludeTableList.count > 0)
	{
		log_error("Filtering setup in \"%s\" contains "
				  "%d entries in section \"%s\" and %d entries in "
				  "section \"%s\", please use only one of these sections.",
				  filename,
				  filters->includeOnlyTableList.count,
				  "include-only-table",
				  filters->excludeTableList.count,
				  "exclude-table");
		return false;
	}

	if (filters->includeOnlyTableList.count > 0 &&
		filters->excludeSchemaList.count > 0)
	{
		log_warn("Filtering setup in \"%s\" contains %d entries "
				 "in \"%s\" section and %d entries in \"%s\" section, "
				 "please make sure not to filter-out schema of "
				 "tables you want to include",
				 filename,
				 filters->includeOnlyTableList.count,
				 "include-only-table",
				 filters->excludeSchemaList.count,
				 "exclude-schema");
	}

	/*
	 * Now assign a proper type to the source filter.
	 */
	if (filters->includeOnlyTableList.count > 0)
	{
		filters->type = SOURCE_FILTER_TYPE_INCL;
	}

	/*
	 * include-only-schema works the same as an exclude-schema filter, it only
	 * allows another spelling of it that might be more useful -- it's still an
	 * exclusion filter.
	 */
	else if (filters->includeOnlySchemaList.count > 0 ||
			 filters->excludeSchemaList.count > 0 ||
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
		log_error("Failed to parse quoted relation name: \"%s\"", qname);
		return false;
	}

	char *nspnameStart = qname[0] == '"' ? (char *) qname + 1 : (char *) qname;
	char *nspnameEnd = *(dot - 1) == '"' ? dot - 1 : dot;

	/* skip last character of the range, either a closing quote or the dot */
	int nsplen = nspnameEnd - nspnameStart;

	size_t nspbytes =
		sformat(table->nspname, sizeof(table->nspname), "%.*s",
				nsplen,
				nspnameStart);

	if (nspbytes >= sizeof(table->nspname))
	{
		log_error("Failed to parse schema name \"%s\" (%d bytes long), "
				  "pgcopydb and Postgres only support names up to %lu bytes",
				  table->nspname,
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
		log_error("Failed to parse quoted relation name: \"%s\"", ptr);
		return false;
	}

	char *relnameStart = ptr[0] == '"' ? ptr + 1 : ptr;
	char *relnameEnd = *(end - 1) == '"' ? end - 1 : end;
	int rellen = relnameEnd - relnameStart + 1;

	size_t relbytes =
		sformat(table->relname, sizeof(table->relname), "%.*s",
				rellen,
				relnameStart);

	if (relbytes >= sizeof(table->relname))
	{
		log_error("Failed to parse relation name \"%s\" (%d bytes long), "
				  "pgcopydb and Postgres only support names up to %lu bytes",
				  table->relname,
				  rellen,
				  sizeof(table->relname));
		return false;
	}

	return true;
}
