/*
 * src/bin/pgcopydb/filtering.c
 *     Implementation of a CLI which lets you run individual routines
 *     directly
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <regex.h>

#include "parson.h"

#include "env_utils.h"
#include "file_utils.h"
#include "ini.h"
#include "log.h"
#include "filtering.h"
#include "parsing_utils.h"
#include "string_utils.h"


static bool parse_filter_quoted_table_name(SourceFilterTable *table,
										   const char *qname);
static bool parse_delimited_re(const char **ptr, char *buf, size_t bufsz);


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

	/*
	 * The index in the sections array matches the SourceFilterSection enum
	 * values.
	 */
	struct section
	{
		char name[NAMEDATALEN];
		SourceFilterSection section;
		SourceFilterTableList *list;
		SourceFilterTablePatternList *patternList;
		SourceFilterSchemaList *schemaList;
		SourceFilterSchemaPatternList *schemaPatternList;
	}
	sections[] =
	{
		{
			"include-only-schema",
			SOURCE_FILTER_INCLUDE_ONLY_SCHEMA,
			NULL, NULL,
			&(filters->includeOnlySchemaList),
			&(filters->includeOnlySchemaPatternList)
		},
		{
			"exclude-schema",
			SOURCE_FILTER_EXCLUDE_SCHEMA,
			NULL, NULL,
			&(filters->excludeSchemaList),
			&(filters->excludeSchemaPatternList)
		},
		{
			"exclude-table",
			SOURCE_FILTER_EXCLUDE_TABLE,
			&(filters->excludeTableList),
			&(filters->excludeTablePatternList),
			NULL, NULL
		},
		{
			"exclude-table-data",
			SOURCE_FILTER_EXCLUDE_TABLE_DATA,
			&(filters->excludeTableDataList),
			&(filters->excludeTableDataPatternList),
			NULL, NULL
		},
		{
			"exclude-index",
			SOURCE_FILTER_EXCLUDE_INDEX,
			&(filters->excludeIndexList),
			&(filters->excludeIndexPatternList),
			NULL, NULL
		},
		{
			"include-only-table",
			SOURCE_FILTER_INCLUDE_ONLY_TABLE,
			&(filters->includeOnlyTableList),
			&(filters->includeOnlyTablePatternList),
			NULL, NULL
		},
		{ "exclude-extension", SOURCE_FILTER_EXCLUDE_EXTENSION, NULL, NULL, NULL, NULL },
		{ "include-only-extension", SOURCE_FILTER_INCLUDE_ONLY_EXTENSION, NULL, NULL,
		  NULL, NULL },
		{ "", SOURCE_FILTER_UNKNOWN, NULL, NULL, NULL, NULL },
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
			/* skip prefix match, only accept full length match */
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
			case SOURCE_FILTER_EXCLUDE_SCHEMA:
			{
				SourceFilterSchemaList *schemaList = sections[i].schemaList;
				SourceFilterSchemaPatternList *patList = sections[i].schemaPatternList;
				const char *secLabel = sections[i].name;

				int exactCount = 0, patCount = 0;

				for (int o = 0; o < optionCount; o++)
				{
					if (filter_entry_is_pattern(ini_property_name(ini, sectionIndex, o)))
					{
						++patCount;
					}
					else
					{
						++exactCount;
					}
				}

				if (exactCount > 0)
				{
					schemaList->array =
						(SourceFilterSchema *) calloc(exactCount,
													  sizeof(SourceFilterSchema));
					if (schemaList->array == NULL)
					{
						log_error(ALLOCATION_FAILED_ERROR);
						return false;
					}
				}

				if (patCount > 0)
				{
					patList->array =
						(SourceFilterSchemaPattern *)
						calloc(patCount, sizeof(SourceFilterSchemaPattern));
					if (patList->array == NULL)
					{
						log_error(ALLOCATION_FAILED_ERROR);
						return false;
					}
				}

				int exactIdx = 0, patIdx = 0;

				for (int o = 0; o < optionCount; o++)
				{
					const char *optionName =
						ini_property_name(ini, sectionIndex, o);

					if (filter_entry_is_pattern(optionName))
					{
						SourceFilterSchemaPattern *pat =
							&(patList->array[patIdx++]);

						if (!parse_filter_schema_pattern(pat, optionName))
						{
							(void) ini_destroy(ini);
							return false;
						}

						log_debug("%s schema pattern \"%s\"",
								  secLabel,
								  pat->nspname_re);
					}
					else
					{
						SourceFilterSchema *schema =
							&(schemaList->array[exactIdx++]);

						strlcpy(schema->nspname, optionName,
								sizeof(schema->nspname));

						/* best-effort; filters_validate_and_normalize will update */
						sformat(schema->restoreListName,
								sizeof(schema->restoreListName),
								"\"%s\"", schema->nspname);

						log_debug("%s schema \"%s\"", secLabel, schema->nspname);
					}
				}

				schemaList->count = exactIdx;
				patList->count = patIdx;

				break;
			}

			case SOURCE_FILTER_EXCLUDE_TABLE:
			case SOURCE_FILTER_EXCLUDE_TABLE_DATA:
			case SOURCE_FILTER_EXCLUDE_INDEX:
			case SOURCE_FILTER_INCLUDE_ONLY_TABLE:
			{
				SourceFilterTableList *list = sections[i].list;
				SourceFilterTablePatternList *patList = sections[i].patternList;
				const char *secLabel = sections[i].name;

				int exactCount = 0, patCount = 0;

				for (int o = 0; o < optionCount; o++)
				{
					if (filter_entry_is_pattern(ini_property_name(ini, sectionIndex, o)))
					{
						++patCount;
					}
					else
					{
						++exactCount;
					}
				}

				if (exactCount > 0)
				{
					list->array =
						(SourceFilterTable *) calloc(exactCount,
													 sizeof(SourceFilterTable));
					if (list->array == NULL)
					{
						log_error(ALLOCATION_FAILED_ERROR);
						return false;
					}
				}

				if (patCount > 0)
				{
					patList->array =
						(SourceFilterTablePattern *)
						calloc(patCount, sizeof(SourceFilterTablePattern));
					if (patList->array == NULL)
					{
						log_error(ALLOCATION_FAILED_ERROR);
						return false;
					}
				}

				int exactIdx = 0, patIdx = 0;

				for (int o = 0; o < optionCount; o++)
				{
					const char *optionName =
						ini_property_name(ini, sectionIndex, o);

					if (filter_entry_is_pattern(optionName))
					{
						SourceFilterTablePattern *pat =
							&(patList->array[patIdx++]);

						if (!parse_filter_table_pattern(pat, optionName))
						{
							(void) ini_destroy(ini);
							return false;
						}

						log_debug("%s pattern nspname=%s%s relname=%s%s",
								  secLabel,
								  pat->nspname[0] ? pat->nspname : "",
								  pat->nspname_re[0] ? pat->nspname_re : "",
								  pat->relname[0] ? pat->relname : "",
								  pat->relname_re[0] ? pat->relname_re : "");
					}
					else
					{
						SourceFilterTable *table = &(list->array[exactIdx++]);

						if (!parse_filter_quoted_table_name(table, optionName))
						{
							(void) ini_destroy(ini);
							return false;
						}

						log_trace("%s \"%s\".\"%s\"",
								  secLabel,
								  table->nspname,
								  table->relname);
					}
				}

				list->count = exactIdx;
				patList->count = patIdx;

				break;
			}

			case SOURCE_FILTER_EXCLUDE_EXTENSION:
			{
				filters->excludeExtensionList.count = optionCount;
				filters->excludeExtensionList.array =
					(SourceFilterExtension *) calloc(optionCount,
													 sizeof(SourceFilterExtension));

				if (filters->excludeExtensionList.array == NULL)
				{
					log_error(ALLOCATION_FAILED_ERROR);
					return false;
				}

				for (int o = 0; o < optionCount; o++)
				{
					SourceFilterExtension *extension =
						&(filters->excludeExtensionList.array[o]);

					const char *optionName =
						ini_property_name(ini, sectionIndex, o);

					strlcpy(extension->extname, optionName,
							sizeof(extension->extname));

					log_debug("excluding extension \"%s\"", extension->extname);
				}
				break;
			}

			case SOURCE_FILTER_INCLUDE_ONLY_EXTENSION:
			{
				filters->includeOnlyExtensionList.count = optionCount;
				filters->includeOnlyExtensionList.array =
					(SourceFilterExtension *) calloc(optionCount,
													 sizeof(SourceFilterExtension));

				if (filters->includeOnlyExtensionList.array == NULL)
				{
					log_error(ALLOCATION_FAILED_ERROR);
					return false;
				}

				for (int o = 0; o < optionCount; o++)
				{
					SourceFilterExtension *extension =
						&(filters->includeOnlyExtensionList.array[o]);

					const char *optionName =
						ini_property_name(ini, sectionIndex, o);

					strlcpy(extension->extname, optionName,
							sizeof(extension->extname));

					log_debug("including only extension \"%s\"", extension->extname);
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

	if (filters->includeOnlyExtensionList.count > 0 &&
		filters->excludeExtensionList.count > 0)
	{
		log_error("Filtering setup in \"%s\" contains %d entries "
				  "in section \"%s\" and %d entries in section \"%s\", "
				  "please use only one of these sections.",
				  filename,
				  filters->includeOnlyExtensionList.count,
				  "include-only-extension",
				  filters->excludeExtensionList.count,
				  "exclude-extension");
		return false;
	}

	/*
	 * Now assign a proper type to the source filter.
	 * Pattern lists are counted the same as their exact equivalents because
	 * filters_validate_and_normalize() expands them into the exact lists before
	 * the SQL queries run.
	 */
	if (filters->includeOnlyTableList.count > 0 ||
		filters->includeOnlyTablePatternList.count > 0)
	{
		filters->type = SOURCE_FILTER_TYPE_INCL;
	}

	/*
	 * include-only-schema works the same as an exclude-schema filter, it only
	 * allows another spelling of it that might be more useful -- it's still an
	 * exclusion filter.
	 */
	else if (filters->includeOnlySchemaList.count > 0 ||
			 filters->includeOnlySchemaPatternList.count > 0 ||
			 filters->excludeSchemaList.count > 0 ||
			 filters->excludeSchemaPatternList.count > 0 ||
			 filters->excludeTableList.count > 0 ||
			 filters->excludeTablePatternList.count > 0 ||
			 filters->excludeTableDataList.count > 0 ||
			 filters->excludeTableDataPatternList.count > 0)
	{
		filters->type = SOURCE_FILTER_TYPE_EXCL;
	}
	else if (filters->excludeIndexList.count > 0 ||
			 filters->excludeIndexPatternList.count > 0)
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
				  "pgcopydb and Postgres only support names up to %zu bytes",
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
				  "pgcopydb and Postgres only support names up to %zu bytes",
				  table->relname,
				  rellen,
				  sizeof(table->relname));
		return false;
	}

	return true;
}


/*
 * copydb_filtering_as_json prepares the filtering setup of the CopyDataSpecs
 * as a JSON object within the given JSON_Value.
 */
bool
filters_as_json(SourceFilters *filters, JSON_Value *jsFilter)
{
	JSON_Object *jsFilterObj = json_value_get_object(jsFilter);

	json_object_set_string(jsFilterObj,
						   "type",
						   filterTypeToString(filters->type));

	/*
	 * Schema exact lists.  When patterns have been expanded (countOriginal > 0)
	 * only serialize the user-supplied entries so the JSON stays stable across
	 * pre- and post-expansion catalog checks.
	 */
	struct schemasection
	{
		char name[PG_NAMEDATALEN];
		char patname[PG_NAMEDATALEN];
		SourceFilterSchemaList *list;
		SourceFilterSchemaPatternList *patList;
	};

	struct schemasection schemasections[] = {
		{
			"include-only-schema",
			"include-only-schema-pattern",
			&(filters->includeOnlySchemaList),
			&(filters->includeOnlySchemaPatternList)
		},
		{
			"exclude-schema",
			"exclude-schema-pattern",
			&(filters->excludeSchemaList),
			&(filters->excludeSchemaPatternList)
		},
		{ "", "", NULL, NULL },
	};

	for (int i = 0; schemasections[i].list != NULL; i++)
	{
		SourceFilterSchemaList *slist = schemasections[i].list;
		SourceFilterSchemaPatternList *splist = schemasections[i].patList;

		int limit = (slist->countOriginal > 0) ? slist->countOriginal : slist->count;

		if (limit > 0)
		{
			JSON_Value *jsSchema = json_value_init_array();
			JSON_Array *jsSchemaArray = json_value_get_array(jsSchema);

			for (int j = 0; j < limit; j++)
			{
				json_array_append_string(jsSchemaArray, slist->array[j].nspname);
			}

			json_object_set_value(jsFilterObj, schemasections[i].name, jsSchema);
		}

		if (splist->count > 0)
		{
			JSON_Value *jsPat = json_value_init_array();
			JSON_Array *jsPatArray = json_value_get_array(jsPat);

			for (int j = 0; j < splist->count; j++)
			{
				json_array_append_string(jsPatArray, splist->array[j].nspname_re);
			}

			json_object_set_value(jsFilterObj, schemasections[i].patname, jsPat);
		}
	}

	/* exclude table lists */
	struct section
	{
		char name[PG_NAMEDATALEN];
		char patname[PG_NAMEDATALEN];
		SourceFilterTableList *list;
		SourceFilterTablePatternList *patList;
	};

	struct section sections[] = {
		{
			"exclude-table",
			"exclude-table-pattern",
			&(filters->excludeTableList),
			&(filters->excludeTablePatternList)
		},
		{
			"exclude-table-data",
			"exclude-table-data-pattern",
			&(filters->excludeTableDataList),
			&(filters->excludeTableDataPatternList)
		},
		{
			"exclude-index",
			"exclude-index-pattern",
			&(filters->excludeIndexList),
			&(filters->excludeIndexPatternList)
		},
		{
			"include-only-table",
			"include-only-table-pattern",
			&(filters->includeOnlyTableList),
			&(filters->includeOnlyTablePatternList)
		},
		{ "", "", NULL, NULL },
	};

	for (int i = 0; sections[i].list != NULL; i++)
	{
		char *sectionName = sections[i].name;
		SourceFilterTableList *list = sections[i].list;
		SourceFilterTablePatternList *patList = sections[i].patList;

		int limit = (list->countOriginal > 0) ? list->countOriginal : list->count;

		if (limit > 0)
		{
			JSON_Value *jsList = json_value_init_array();
			JSON_Array *jsListArray = json_value_get_array(jsList);

			for (int j = 0; j < limit; j++)
			{
				SourceFilterTable *table = &(list->array[j]);

				JSON_Value *jsTable = json_value_init_object();
				JSON_Object *jsTableObj = json_value_get_object(jsTable);

				json_object_set_string(jsTableObj, "schema", table->nspname);
				json_object_set_string(jsTableObj, "name", table->relname);

				json_array_append_value(jsListArray, jsTable);
			}

			json_object_set_value(jsFilterObj, sectionName, jsList);
		}

		if (patList->count > 0)
		{
			JSON_Value *jsPat = json_value_init_array();
			JSON_Array *jsPatArray = json_value_get_array(jsPat);

			for (int j = 0; j < patList->count; j++)
			{
				SourceFilterTablePattern *pat = &(patList->array[j]);

				JSON_Value *jsPEntry = json_value_init_object();
				JSON_Object *jsPEntryObj = json_value_get_object(jsPEntry);

				if (pat->nspname[0] != '\0')
				{
					json_object_set_string(jsPEntryObj, "schema", pat->nspname);
				}

				if (pat->nspname_re[0] != '\0')
				{
					json_object_set_string(jsPEntryObj, "schema-re",
										   pat->nspname_re);
				}

				if (pat->relname[0] != '\0')
				{
					json_object_set_string(jsPEntryObj, "name", pat->relname);
				}

				if (pat->relname_re[0] != '\0')
				{
					json_object_set_string(jsPEntryObj, "name-re", pat->relname_re);
				}

				json_array_append_value(jsPatArray, jsPEntry);
			}

			json_object_set_value(jsFilterObj, sections[i].patname, jsPat);
		}
	}

	/* extension filter lists */
	struct extsection
	{
		char name[PG_NAMEDATALEN];
		SourceFilterExtensionList *list;
	};

	struct extsection extsections[] = {
		{ "exclude-extension", &(filters->excludeExtensionList) },
		{ "include-only-extension", &(filters->includeOnlyExtensionList) },
		{ "", NULL },
	};

	for (int i = 0; extsections[i].list != NULL; i++)
	{
		char *sectionName = extsections[i].name;
		SourceFilterExtensionList *list = extsections[i].list;

		if (list->count > 0)
		{
			JSON_Value *jsList = json_value_init_array();
			JSON_Array *jsListArray = json_value_get_array(jsList);

			for (int j = 0; j < list->count; j++)
			{
				json_array_append_string(jsListArray, list->array[j].extname);
			}

			json_object_set_value(jsFilterObj, sectionName, jsList);
		}
	}

	return true;
}


/*
 * filter_entry_is_pattern returns true when an INI filter entry contains a
 * ~/pattern/ regex literal outside of a double-quoted name.
 */
bool
filter_entry_is_pattern(const char *entry)
{
	if (entry == NULL)
	{
		return false;
	}

	bool inQuotes = false;

	for (const char *p = entry; *p; ++p)
	{
		if (*p == '"')
		{
			inQuotes = !inQuotes;
		}
		else if (!inQuotes && *p == '~')
		{
			return true;
		}
	}

	return false;
}


/*
 * parse_delimited_re parses one ~/.../ (or ~[...], ~{...}, ~(...), ~<...>,
 * ~"...", ~'...', ~|...|, ~#...#, ~$...$) regex literal from *ptr and writes
 * the pattern text into buf (without the delimiters).  *ptr is advanced past
 * the closing delimiter on success.
 *
 * Multiple consecutive segments can be concatenated by the caller via repeated
 * calls:  ~/foo/~/bar/  ->  "foo" + "bar"  ->  "foobar".
 */
static bool
parse_delimited_re(const char **ptr, char *buf, size_t bufsz)
{
	const char *p = *ptr;

	if (p[0] != '~')
	{
		log_error("Expected '~' at start of regex pattern, got '%c'", p[0]);
		return false;
	}

	++p;

	char open = p[0];
	char close;

	switch (open)
	{
		case '/':
		{
			close = '/';
			break;
		}

		case '[':
		{
			close = ']';
			break;
		}

		case '{':
		{
			close = '}';
			break;
		}

		case '(':
		{
			close = ')';
			break;
		}

		case '<':
		{
			close = '>';
			break;
		}

		case '"':
		{
			close = '"';
			break;
		}

		case '\'':
		{
			close = '\'';
			break;
		}

		case '|':
		{
			close = '|';
			break;
		}

		case '#':
		{
			close = '#';
			break;
		}

		case '$':
		{
			close = '$';
			break;
		}

		default:
		{
			log_error("Unrecognised regex delimiter '%c' after '~'", open);
			return false;
		}
	}

	++p; /* skip the opening delimiter */

	const char *start = p;

	while (*p && *p != close)
	{
		++p;
	}

	if (*p == '\0')
	{
		log_error("Unterminated regex pattern: closing '%c' not found", close);
		return false;
	}

	int len = (int) (p - start);

	if ((size_t) len >= bufsz)
	{
		log_error("Regex pattern is too long (%d bytes, max %zu)", len, bufsz - 1);
		return false;
	}

	sformat(buf, bufsz, "%.*s", len, start);

	*ptr = p + 1; /* skip the closing delimiter */

	return true;
}


/*
 * parse_filter_schema_pattern parses a schema-only pattern entry such as
 * ~/staging_/ into a SourceFilterSchemaPattern.  Multiple consecutive
 * ~/.../ segments are concatenated before compilation.
 */
bool
parse_filter_schema_pattern(SourceFilterSchemaPattern *pat, const char *entry)
{
	const char *p = entry;
	char re[BUFSIZE] = { 0 };
	size_t relen = 0;

	while (p[0] == '~')
	{
		char segment[BUFSIZE] = { 0 };

		if (!parse_delimited_re(&p, segment, sizeof(segment)))
		{
			return false;
		}

		size_t seglen = strlen(segment);

		if (relen + seglen >= sizeof(re))
		{
			log_error("Schema pattern is too long in filter entry: \"%s\"",
					  entry);
			return false;
		}

		sformat(re + relen, sizeof(re) - relen, "%s", segment);
		relen += seglen;
	}

	if (p[0] != '\0')
	{
		log_error("Unexpected characters after schema pattern in: \"%s\"", p);
		return false;
	}

	if (re[0] == '\0')
	{
		log_error("Empty schema pattern in filter entry: \"%s\"", entry);
		return false;
	}

	strlcpy(pat->nspname_re, re, sizeof(pat->nspname_re));

	/* Validate the regex at parse time; actual matching is done server-side. */
	regex_t compiled;
	int rc = regcomp(&compiled, re, REG_EXTENDED | REG_NOSUB);

	if (rc != 0)
	{
		char errbuf[256];

		(void) regerror(rc, &compiled, errbuf, sizeof(errbuf));
		log_error("Failed to compile schema regex \"%s\": %s", re, errbuf);
		return false;
	}

	regfree(&compiled);

	return true;
}


/*
 * parse_filter_table_pattern parses a table pattern entry from an INI filter
 * file.  The entry has the form  <schema_part>.<table_part>  where either
 * part may be a ~/regex/ literal or an exact (possibly quoted) identifier:
 *
 *   public.~/^tmp_/             exact schema, regex table
 *   ~/staging_/.orders          regex schema, exact table
 *   ~/staging_/.~/^tmp_/        regex schema, regex table
 *
 * Multiple consecutive ~/.../ segments are concatenated:
 *   ~/^tmp_/~/[0-9]+$/  ->  relname_re = "^tmp_[0-9]+$"
 *
 * Exactly one of (nspname, nspname_re) is non-empty after a successful parse,
 * and exactly one of (relname, relname_re) is non-empty.
 */
bool
parse_filter_table_pattern(SourceFilterTablePattern *pat, const char *entry)
{
	const char *p = entry;

	/* ---- schema part ---- */
	if (p[0] == '~')
	{
		char re[BUFSIZE] = { 0 };
		size_t relen = 0;

		do {
			char segment[BUFSIZE] = { 0 };

			if (!parse_delimited_re(&p, segment, sizeof(segment)))
			{
				return false;
			}

			size_t seglen = strlen(segment);

			if (relen + seglen >= sizeof(re))
			{
				log_error("Schema pattern too long in filter entry: \"%s\"",
						  entry);
				return false;
			}

			sformat(re + relen, sizeof(re) - relen, "%s", segment);
			relen += seglen;
		} while (p[0] == '~');

		if (p[0] != '.')
		{
			log_error("Expected '.' separator between schema pattern and table "
					  "in filter entry: \"%s\"", entry);
			return false;
		}

		strlcpy(pat->nspname_re, re, sizeof(pat->nspname_re));

		/* Validate the regex at parse time; actual matching is server-side. */
		regex_t compiled;
		int rc = regcomp(&compiled, re, REG_EXTENDED | REG_NOSUB);

		if (rc != 0)
		{
			char errbuf[256];

			(void) regerror(rc, &compiled, errbuf, sizeof(errbuf));
			log_error("Failed to compile schema regex \"%s\": %s", re, errbuf);
			return false;
		}

		regfree(&compiled);

		++p; /* skip '.' */
	}
	else
	{
		/* Exact schema name: quoted or bare, up to first unquoted '.' */
		const char *nsp = p;
		bool inQ = false;

		while (*p && !(!inQ && *p == '.'))
		{
			if (*p == '"')
			{
				inQ = !inQ;
			}
			++p;
		}

		if (*p != '.')
		{
			log_error("Expected '.' separator between schema name and table "
					  "in filter entry: \"%s\"", entry);
			return false;
		}

		int nsplen = (int) (p - nsp);

		/* Strip surrounding double-quotes if present */
		if (nsp[0] == '"' && nsplen >= 2 && nsp[nsplen - 1] == '"')
		{
			nsp++;
			nsplen -= 2;
		}

		if ((size_t) nsplen >= sizeof(pat->nspname))
		{
			log_error("Schema name too long in filter entry: \"%s\"", entry);
			return false;
		}

		sformat(pat->nspname, sizeof(pat->nspname), "%.*s", nsplen, nsp);

		++p; /* skip '.' */
	}

	/* ---- table part ---- */
	if (p[0] == '~')
	{
		char re[BUFSIZE] = { 0 };
		size_t relen = 0;

		do {
			char segment[BUFSIZE] = { 0 };

			if (!parse_delimited_re(&p, segment, sizeof(segment)))
			{
				return false;
			}

			size_t seglen = strlen(segment);

			if (relen + seglen >= sizeof(re))
			{
				log_error("Table pattern too long in filter entry: \"%s\"",
						  entry);
				return false;
			}

			sformat(re + relen, sizeof(re) - relen, "%s", segment);
			relen += seglen;
		} while (p[0] == '~');

		if (p[0] != '\0')
		{
			log_error("Unexpected characters after table pattern in: \"%s\"",
					  entry);
			return false;
		}

		strlcpy(pat->relname_re, re, sizeof(pat->relname_re));

		/* Validate the regex at parse time; actual matching is server-side. */
		regex_t compiled;
		int rc = regcomp(&compiled, re, REG_EXTENDED | REG_NOSUB);

		if (rc != 0)
		{
			char errbuf[256];

			(void) regerror(rc, &compiled, errbuf, sizeof(errbuf));
			log_error("Failed to compile table regex \"%s\": %s", re, errbuf);
			return false;
		}

		regfree(&compiled);
	}
	else
	{
		/* Exact table name: quoted or bare */
		const char *rel = p;
		int rellen = (int) strlen(rel);

		/* Strip surrounding double-quotes if present */
		if (rel[0] == '"' && rellen >= 2 && rel[rellen - 1] == '"')
		{
			rel++;
			rellen -= 2;
		}

		if ((size_t) rellen >= sizeof(pat->relname))
		{
			log_error("Table name too long in filter entry: \"%s\"", entry);
			return false;
		}

		sformat(pat->relname, sizeof(pat->relname), "%.*s", rellen, rel);
	}

	return true;
}
