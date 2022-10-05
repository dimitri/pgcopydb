/*
 * src/bin/pgcopydb/schema.c
 *	 SQL queries to discover the source database schema
 */

#include <inttypes.h>
#include <limits.h>
#include <sys/select.h>
#include <time.h>
#include <unistd.h>

#include "defaults.h"
#include "env_utils.h"
#include "file_utils.h"
#include "filtering.h"
#include "log.h"
#include "parsing_utils.h"
#include "pg_depend_sql.h"
#include "pgsql.h"
#include "schema.h"
#include "signals.h"
#include "string_utils.h"


static bool prepareFilters(PGSQL *pgsql, SourceFilters *filters);
static bool prepareFilterCopyExcludeSchema(PGSQL *pgsql, SourceFilters *filters);
static bool prepareFilterCopyTableList(PGSQL *pgsql,
									   SourceFilterTableList *tableList,
									   const char *temp_table_name);


/* Context used when fetching schema definitions */
typedef struct SourceSchemaArrayContext
{
	char sqlstate[SQLSTATE_LENGTH];
	SourceSchemaArray *schemaArray;
	bool parsedOk;
} SourceSchemaArrayContext;

/* Context used when fetching all the extension definitions */
typedef struct SourceExtensionArrayContext
{
	char sqlstate[SQLSTATE_LENGTH];
	SourceExtensionArray *extensionArray;
	bool parsedOk;
} SourceExtensionArrayContext;

/* Context used when fetching all the table definitions */
typedef struct SourceTableArrayContext
{
	char sqlstate[SQLSTATE_LENGTH];
	SourceTableArray *tableArray;
	bool parsedOk;
} SourceTableArrayContext;

/* Context used when fetching all the sequence definitions */
typedef struct SourceSequenceArrayContext
{
	char sqlstate[SQLSTATE_LENGTH];
	SourceSequenceArray *sequenceArray;
	bool parsedOk;
} SourceSequenceArrayContext;

/* Context used when fetching all the indexes definitions */
typedef struct SourceIndexArrayContext
{
	char sqlstate[SQLSTATE_LENGTH];
	SourceIndexArray *indexArray;
	bool parsedOk;
} SourceIndexArrayContext;

/* Context used when fetching all the table dependencies */
typedef struct SourceDependArrayContext
{
	char sqlstate[SQLSTATE_LENGTH];
	SourceDependArray *dependArray;
	bool parsedOk;
} SourceDependArrayContext;

/* Context used when fetching a list of COPY partitions for a table */
typedef struct SourcePartitionContext
{
	char sqlstate[SQLSTATE_LENGTH];
	SourceTable *table;
	bool parsedOk;
} SourcePartitionContext;

static void getSchemaList(void *ctx, PGresult *result);

static void getExtensionList(void *ctx, PGresult *result);

static bool parseCurrentExtension(PGresult *result,
								  int rowNumber,
								  SourceExtension *extension,
								  int *confIndex);

static bool parseCurrentExtensionConfig(PGresult *result,
										int rowNumber,
										SourceExtensionConfig *extConfig);

static void getTableArray(void *ctx, PGresult *result);

static bool parseCurrentSourceTable(PGresult *result,
									int rowNumber,
									SourceTable *table);

static void getSequenceArray(void *ctx, PGresult *result);

static bool parseCurrentSourceSequence(PGresult *result,
									   int rowNumber,
									   SourceSequence *table);

static void getIndexArray(void *ctx, PGresult *result);

static bool parseCurrentSourceIndex(PGresult *result,
									int rowNumber,
									SourceIndex *index);

static void getDependArray(void *ctx, PGresult *result);

static bool parseCurrentSourceDepend(PGresult *result,
									 int rowNumber,
									 SourceDepend *depend);

static void getPartitionList(void *ctx, PGresult *result);

static bool parseCurrentPartition(PGresult *result,
								  int rowNumber,
								  SourceTableParts *parts);

struct FilteringQueries
{
	SourceFilterType type;
	char *sql;
};


/*
 * schema_list_extensions grabs the list of extensions from the given source
 * Postgres instance and allocates a SourceExtension array with the result of
 * the query.
 */
bool
schema_list_extensions(PGSQL *pgsql, SourceExtensionArray *extArray)
{
	SourceExtensionArrayContext parseContext = { { 0 }, extArray, false };

	char *sql =
		"select e.oid, extname, extnamespace::regnamespace, extrelocatable, "
		"       0 as count, null as n, "
		"       null as extconfig, null as nspname, null as relname, "
		"       null as extcondition "
		"  from pg_extension e "
		" where extconfig is null "

		" UNION ALL "

		"  select e.oid, extname, extnamespace::regnamespace, extrelocatable, "
		"         array_length(e.extconfig, 1) as count, "
		"         extconfig.n, "
		"         extconfig.extconfig, n.nspname, c.relname, "
		"         extcondition[extconfig.n] "
		"    from pg_extension e, "
		"         unnest(extconfig) with ordinality as extconfig(extconfig, n) "
		"          left join pg_class c on c.oid = extconfig.extconfig "
		"          join pg_namespace n on c.relnamespace = n.oid "
		"   where extconfig.extconfig is not null "

		"order by oid, n";

	if (!pgsql_execute_with_params(pgsql, sql,
								   0, NULL, NULL,
								   &parseContext, &getExtensionList))
	{
		log_error("Failed to list extensions");
		return false;
	}

	if (!parseContext.parsedOk)
	{
		log_error("Failed to list extensions");
		return false;
	}

	return true;
}


/*
 * schema_list_extensions grabs the list of extensions from the given source
 * Postgres instance and allocates a SourceExtension array with the result of
 * the query.
 */
bool
schema_list_ext_schemas(PGSQL *pgsql, SourceSchemaArray *array)
{
	SourceSchemaArrayContext parseContext = { { 0 }, array, false };

	char *sql =
		"select n.oid, n.nspname, "
		"       format('- %s %s', "
		"                regexp_replace(n.nspname, '[\n\r]', ' '), "
		"                regexp_replace(auth.rolname, '[\n\r]', ' ')) "
		"  from pg_namespace n "
		"       join pg_roles auth ON auth.oid = n.nspowner "
		"       join pg_depend d "
		"         on d.refclassid = 'pg_namespace'::regclass "
		"        and d.refobjid = n.oid "
		"        and d.classid = 'pg_extension'::regclass "
		" where nspname <> 'public' and nspname !~ '^pg_'";

	if (!pgsql_execute_with_params(pgsql, sql,
								   0, NULL, NULL,
								   &parseContext, &getSchemaList))
	{
		log_error("Failed to list schemas that extensions depend on");
		return false;
	}

	if (!parseContext.parsedOk)
	{
		log_error("Failed to list schemas that extensions depend on");
		return false;
	}

	return true;
}


/*
 * For code simplicity the index array is also the SourceFilterType enum value.
 */
struct FilteringQueries listSourceTablesSQL[] = {
	{
		SOURCE_FILTER_TYPE_NONE,

		"  select c.oid, n.nspname, c.relname, c.reltuples::bigint, "
		"         pg_table_size(c.oid) as bytes, "
		"         pg_size_pretty(pg_table_size(c.oid)), "
		"         false as excludedata, "
		"         format('%s %s %s', "
		"                regexp_replace(n.nspname, '[\n\r]', ' '), "
		"                regexp_replace(c.relname, '[\n\r]', ' '), "
		"                regexp_replace(auth.rolname, '[\n\r]', ' ')), "
		"         pkeys.attname as partkey"

		"    from pg_catalog.pg_class c"
		"         join pg_catalog.pg_namespace n on c.relnamespace = n.oid"
		"         join pg_roles auth ON auth.oid = c.relowner"

		/* find a copy partition key candidate */
		"         left join lateral ("
		"             select indrelid, indexrelid, a.attname"

		"               from pg_index x"
		"               join pg_class i on i.oid = x.indexrelid"
		"               join pg_attribute a on a.attrelid = c.oid and attnum = 1"

		"              where x.indrelid = c.oid"
		"                and (indisprimary or indisunique)"
		"                and array_length(indkey::integer[], 1) = 1"
		"                and atttypid in ('smallint'::regtype,"
		"                                 'int'::regtype,"
		"                                 'bigint'::regtype)"
		"           order by not indisprimary, not indisunique"
		"              limit 1"
		"         ) as pkeys on true"

		"   where relkind = 'r' and c.relpersistence = 'p' "
		"     and n.nspname !~ '^pg_' and n.nspname <> 'information_schema' "

		/* avoid pg_class entries which belong to extensions */
		"     and not exists "
		"       ( "
		"         select 1 "
		"           from pg_depend d "
		"          where d.classid = 'pg_class'::regclass "
		"            and d.objid = c.oid "
		"            and d.deptype = 'e' "
		"       ) "

		"order by bytes desc, n.nspname, c.relname"
	},

	{
		SOURCE_FILTER_TYPE_INCL,

		"  select c.oid, n.nspname, c.relname, c.reltuples::bigint, "
		"         pg_table_size(c.oid) as bytes, "
		"         pg_size_pretty(pg_table_size(c.oid)), "
		"         exists(select 1 "
		"                  from pg_temp.filter_exclude_table_data ftd "
		"                 where n.nspname = ftd.nspname "
		"                   and c.relname = ftd.relname) as excludedata,"
		"         format('%s %s %s', "
		"                regexp_replace(n.nspname, '[\n\r]', ' '), "
		"                regexp_replace(c.relname, '[\n\r]', ' '), "
		"                regexp_replace(auth.rolname, '[\n\r]', ' ')), "
		"         pkeys.attname as partkey"

		"    from pg_catalog.pg_class c "
		"         join pg_catalog.pg_namespace n on c.relnamespace = n.oid "
		"         join pg_roles auth ON auth.oid = c.relowner"

		/* include-only-table */
		"         join pg_temp.filter_include_only_table inc "
		"           on n.nspname = inc.nspname "
		"          and c.relname = inc.relname "

		/* find a copy partition key candidate */
		"         left join lateral ("
		"             select indrelid, indexrelid, a.attname"

		"               from pg_index x"
		"               join pg_class i on i.oid = x.indexrelid"
		"               join pg_attribute a on a.attrelid = c.oid and attnum = 1"

		"              where x.indrelid = c.oid"
		"                and (indisprimary or indisunique)"
		"                and array_length(indkey::integer[], 1) = 1"
		"                and atttypid in ('smallint'::regtype,"
		"                                 'int'::regtype,"
		"                                 'bigint'::regtype)"
		"           order by not indisprimary, not indisunique"
		"              limit 1"
		"         ) as pkeys on true"

		"   where relkind = 'r' and c.relpersistence = 'p' "
		"     and n.nspname !~ '^pg_' and n.nspname <> 'information_schema' "

		/* avoid pg_class entries which belong to extensions */
		"     and not exists "
		"       ( "
		"         select 1 "
		"           from pg_depend d "
		"          where d.classid = 'pg_class'::regclass "
		"            and d.objid = c.oid "
		"            and d.deptype = 'e' "
		"       ) "

		"order by bytes desc, n.nspname, c.relname"
	},

	{
		SOURCE_FILTER_TYPE_EXCL,

		"  select c.oid, n.nspname, c.relname, c.reltuples::bigint, "
		"         pg_table_size(c.oid) as bytes, "
		"         pg_size_pretty(pg_table_size(c.oid)), "
		"         ftd.relname is not null as excludedata, "
		"         format('%s %s %s', "
		"                regexp_replace(n.nspname, '[\n\r]', ' '), "
		"                regexp_replace(c.relname, '[\n\r]', ' '), "
		"                regexp_replace(auth.rolname, '[\n\r]', ' ')), "
		"         pkeys.attname as partkey"

		"    from pg_catalog.pg_class c "
		"         join pg_catalog.pg_namespace n on c.relnamespace = n.oid "
		"         join pg_roles auth ON auth.oid = c.relowner"

		/* exclude-schema */
		"         left join pg_temp.filter_exclude_schema fn "
		"                on n.nspname = fn.nspname "

		/* exclude-table */
		"         left join pg_temp.filter_exclude_table ft "
		"                on n.nspname = ft.nspname "
		"               and c.relname = ft.relname "

		/* exclude-table-data */
		"         left join pg_temp.filter_exclude_table_data ftd "
		"                on n.nspname = ftd.nspname "
		"               and c.relname = ftd.relname "

		/* find a copy partition key candidate */
		"         left join lateral ("
		"             select indrelid, indexrelid, a.attname"

		"               from pg_index x"
		"               join pg_class i on i.oid = x.indexrelid"
		"               join pg_attribute a on a.attrelid = c.oid and attnum = 1"

		"              where x.indrelid = c.oid"
		"                and (indisprimary or indisunique)"
		"                and array_length(indkey::integer[], 1) = 1"
		"                and atttypid in ('smallint'::regtype,"
		"                                 'int'::regtype,"
		"                                 'bigint'::regtype)"
		"           order by not indisprimary, not indisunique"
		"              limit 1"
		"         ) as pkeys on true"

		"   where relkind in ('r', 'p') and c.relpersistence = 'p' "
		"     and n.nspname !~ '^pg_' and n.nspname <> 'information_schema' "

		/* WHERE clause for exclusion filters */
		"     and fn.nspname is null "
		"     and ft.relname is null "
		"     and ftd.relname is null "

		/* avoid pg_class entries which belong to extensions */
		"     and not exists "
		"       ( "
		"         select 1 "
		"           from pg_depend d "
		"          where d.classid = 'pg_class'::regclass "
		"            and d.objid = c.oid "
		"            and d.deptype = 'e' "
		"       ) "

		"order by bytes desc, n.nspname, c.relname"
	},

	{
		SOURCE_FILTER_TYPE_LIST_NOT_INCL,

		"  select c.oid, n.nspname, c.relname, c.reltuples::bigint, "
		"         pg_table_size(c.oid) as bytes, "
		"         pg_size_pretty(pg_table_size(c.oid)), "
		"         false as excludedata, "
		"         format('%s %s %s', "
		"                regexp_replace(n.nspname, '[\n\r]', ' '), "
		"                regexp_replace(c.relname, '[\n\r]', ' '), "
		"                regexp_replace(auth.rolname, '[\n\r]', ' ')), "
		"         pkeys.attname as partkey"

		"    from pg_catalog.pg_class c "
		"         join pg_catalog.pg_namespace n on c.relnamespace = n.oid "
		"         join pg_roles auth ON auth.oid = c.relowner"

		/* include-only-table */
		"    left join pg_temp.filter_include_only_table inc "
		"           on n.nspname = inc.nspname "
		"          and c.relname = inc.relname "

		/* find a copy partition key candidate */
		"         left join lateral ("
		"             select indrelid, indexrelid, a.attname"

		"               from pg_index x"
		"               join pg_class i on i.oid = x.indexrelid"
		"               join pg_attribute a on a.attrelid = c.oid and attnum = 1"

		"              where x.indrelid = c.oid"
		"                and (indisprimary or indisunique)"
		"                and array_length(indkey::integer[], 1) = 1"
		"                and atttypid in ('smallint'::regtype,"
		"                                 'int'::regtype,"
		"                                 'bigint'::regtype)"
		"           order by not indisprimary, not indisunique"
		"              limit 1"
		"         ) as pkeys on true"

		"   where relkind in ('r', 'p') and c.relpersistence = 'p' "
		"     and n.nspname !~ '^pg_' and n.nspname <> 'information_schema' "

		/* WHERE clause for exclusion filters */
		"     and inc.nspname is null "

		/* avoid pg_class entries which belong to extensions */
		"     and not exists "
		"       ( "
		"         select 1 "
		"           from pg_depend d "
		"          where d.classid = 'pg_class'::regclass "
		"            and d.objid = c.oid "
		"            and d.deptype = 'e' "
		"       ) "

		"order by bytes desc, n.nspname, c.relname"
	},

	{
		SOURCE_FILTER_TYPE_LIST_EXCL,

		"  select c.oid, n.nspname, c.relname, c.reltuples::bigint, "
		"         pg_table_size(c.oid) as bytes, "
		"         pg_size_pretty(pg_table_size(c.oid)), "
		"         false as excludedata, "
		"         format('%s %s %s', "
		"                regexp_replace(n.nspname, '[\n\r]', ' '), "
		"                regexp_replace(c.relname, '[\n\r]', ' '), "
		"                regexp_replace(auth.rolname, '[\n\r]', ' ')), "
		"         pkeys.attname as partkey"

		"    from pg_catalog.pg_class c "
		"         join pg_catalog.pg_namespace n on c.relnamespace = n.oid "
		"         join pg_roles auth ON auth.oid = c.relowner"

		/* exclude-schema */
		"         left join pg_temp.filter_exclude_schema fn "
		"                on n.nspname = fn.nspname "

		/* exclude-table */
		"         left join pg_temp.filter_exclude_table ft "
		"                on n.nspname = ft.nspname "
		"               and c.relname = ft.relname "

		/* find a copy partition key candidate */
		"         left join lateral ("
		"             select indrelid, indexrelid, a.attname"

		"               from pg_index x"
		"               join pg_class i on i.oid = x.indexrelid"
		"               join pg_attribute a on a.attrelid = c.oid and attnum = 1"

		"              where x.indrelid = c.oid"
		"                and (indisprimary or indisunique)"
		"                and array_length(indkey::integer[], 1) = 1"
		"                and atttypid in ('smallint'::regtype,"
		"                                 'int'::regtype,"
		"                                 'bigint'::regtype)"
		"           order by not indisprimary, not indisunique"
		"              limit 1"
		"         ) as pkeys on true"

		"   where relkind in ('r', 'p') and c.relpersistence = 'p' "
		"     and n.nspname !~ '^pg_' and n.nspname <> 'information_schema' "

		/* WHERE clause for exclusion filters */
		"     and (   fn.nspname is not null "
		"          or ft.relname is not null ) "

		/* avoid pg_class entries which belong to extensions */
		"     and not exists "
		"       ( "
		"         select 1 "
		"           from pg_depend d "
		"          where d.classid = 'pg_class'::regclass "
		"            and d.objid = c.oid "
		"            and d.deptype = 'e' "
		"       ) "

		"order by bytes desc, n.nspname, c.relname"
	}
};


/*
 * schema_list_ordinary_tables grabs the list of tables from the given source
 * Postgres instance and allocates a SourceTable array with the result of the
 * query.
 */
bool
schema_list_ordinary_tables(PGSQL *pgsql,
							SourceFilters *filters,
							SourceTableArray *tableArray)
{
	SourceTableArrayContext context = { { 0 }, tableArray, false };

	log_trace("schema_list_ordinary_tables");

	switch (filters->type)
	{
		case SOURCE_FILTER_TYPE_NONE:
		{
			/* skip filters preparing (temp tables) */
			break;
		}

		case SOURCE_FILTER_TYPE_INCL:
		case SOURCE_FILTER_TYPE_EXCL:
		case SOURCE_FILTER_TYPE_LIST_NOT_INCL:
		case SOURCE_FILTER_TYPE_LIST_EXCL:
		{
			if (!prepareFilters(pgsql, filters))
			{
				log_error("Failed to prepare pgcopydb filters, "
						  "see above for details");
				return false;
			}
			break;
		}

		/* SOURCE_FILTER_TYPE_EXCL_INDEX etc */
		default:
		{
			log_error("BUG: schema_list_ordinary_tables called with "
					  "filtering type %d",
					  filters->type);
			return false;
		}
	}

	log_debug("listSourceTablesSQL[%s]", filterTypeToString(filters->type));

	char *sql = listSourceTablesSQL[filters->type].sql;

	if (!pgsql_execute_with_params(pgsql, sql, 0, NULL, NULL,
								   &context, &getTableArray))
	{
		log_error("Failed to list tables");
		return false;
	}

	if (!context.parsedOk)
	{
		log_error("Failed to list tables");
		return false;
	}

	return true;
}


/*
 * For code simplicity the index array is also the SourceFilterType enum value.
 */
struct FilteringQueries listSourceTablesNoPKSQL[] = {
	{
		SOURCE_FILTER_TYPE_NONE,

		"  select r.oid, n.nspname, r.relname, r.reltuples::bigint, "
		"         pg_table_size(r.oid) as bytes, "
		"         pg_size_pretty(pg_table_size(r.oid)), "
		"         false as excludedata, "
		"         format('%s %s %s', "
		"                regexp_replace(n.nspname, '[\n\r]', ' '), "
		"                regexp_replace(r.relname, '[\n\r]', ' '), "
		"                regexp_replace(auth.rolname, '[\n\r]', ' '))"

		"    from pg_class r "
		"         join pg_namespace n ON n.oid = r.relnamespace "
		"         join pg_roles auth ON auth.oid = r.relowner"

		"   where r.relkind = 'r' and r.relpersistence = 'p'  "
		"     and n.nspname !~ '^pg_' and n.nspname <> 'information_schema' "
		"     and not exists "
		"         ( "
		"           select c.oid "
		"             from pg_constraint c "
		"            where c.conrelid = r.oid "
		"              and c.contype = 'p' "
		"         ) "

		/* avoid pg_class entries which belong to extensions */
		"     and not exists "
		"       ( "
		"         select 1 "
		"           from pg_depend d "
		"          where d.classid = 'pg_class'::regclass "
		"            and d.objid = c.oid "
		"            and d.deptype = 'e' "
		"       ) "

		"order by n.nspname, r.relname"
	},

	{
		SOURCE_FILTER_TYPE_INCL,

		"  select r.oid, n.nspname, r.relname, r.reltuples::bigint, "
		"         pg_table_size(r.oid) as bytes, "
		"         pg_size_pretty(pg_table_size(r.oid)), "
		"         false as excludedata, "
		"         format('%s %s %s', "
		"                regexp_replace(n.nspname, '[\n\r]', ' '), "
		"                regexp_replace(r.relname, '[\n\r]', ' '), "
		"                regexp_replace(auth.rolname, '[\n\r]', ' '))"

		"    from pg_class r "
		"         join pg_namespace n ON n.oid = r.relnamespace "
		"         join pg_roles auth ON auth.oid = r.relowner"

		/* include-only-table */
		"         join pg_temp.filter_include_only_table inc "
		"           on n.nspname = inc.nspname "
		"          and r.relname = inc.relname "

		"   where r.relkind = 'r' and r.relpersistence = 'p'  "
		"     and n.nspname !~ '^pg_' and n.nspname <> 'information_schema' "
		"     and not exists "
		"         ( "
		"           select c.oid "
		"             from pg_constraint c "
		"            where c.conrelid = r.oid "
		"              and c.contype = 'p' "
		"         ) "

		/* avoid pg_class entries which belong to extensions */
		"     and not exists "
		"       ( "
		"         select 1 "
		"           from pg_depend d "
		"          where d.classid = 'pg_class'::regclass "
		"            and d.objid = c.oid "
		"            and d.deptype = 'e' "
		"       ) "

		"order by n.nspname, r.relname"
	},

	{
		SOURCE_FILTER_TYPE_EXCL,

		"  select r.oid, n.nspname, r.relname, r.reltuples::bigint, "
		"         pg_table_size(r.oid) as bytes, "
		"         pg_size_pretty(pg_table_size(r.oid)), "
		"         ftd.relname is not null as excludedata, "
		"         format('%s %s %s', "
		"                regexp_replace(n.nspname, '[\n\r]', ' '), "
		"                regexp_replace(r.relname, '[\n\r]', ' '), "
		"                regexp_replace(auth.rolname, '[\n\r]', ' '))"
		"    from pg_class r "
		"         join pg_namespace n ON n.oid = r.relnamespace "
		"         join pg_roles auth ON auth.oid = r.relowner"

		/* exclude-schema */
		"         left join pg_temp.filter_exclude_schema fn "
		"                on n.nspname = fn.nspname "

		/* exclude-table */
		"         left join pg_temp.filter_exclude_table ft "
		"                on n.nspname = ft.nspname "
		"               and r.relname = ft.relname "

		/* exclude-table-data */
		"         left join pg_temp.filter_exclude_table_data ftd "
		"                on n.nspname = ftd.nspname "
		"               and r.relname = ftd.relname "

		"   where r.relkind = 'r' and r.relpersistence = 'p'  "
		"     and n.nspname !~ '^pg_' and n.nspname <> 'information_schema' "
		"     and not exists "
		"         ( "
		"           select c.oid "
		"             from pg_constraint c "
		"            where c.conrelid = r.oid "
		"              and c.contype = 'p' "
		"         ) "

		/* WHERE clause for exclusion filters */
		"     and fn.nspname is null "
		"     and ft.relname is null "
		"     and ftd.relname is null "

		/* avoid pg_class entries which belong to extensions */
		"     and not exists "
		"       ( "
		"         select 1 "
		"           from pg_depend d "
		"          where d.classid = 'pg_class'::regclass "
		"            and d.objid = c.oid "
		"            and d.deptype = 'e' "
		"       ) "

		"order by n.nspname, r.relname"
	},

	{
		SOURCE_FILTER_TYPE_LIST_NOT_INCL,

		"  select r.oid, n.nspname, r.relname, r.reltuples::bigint, "
		"         pg_table_size(r.oid) as bytes, "
		"         pg_size_pretty(pg_table_size(r.oid)), "
		"         false as excludedata, "
		"         format('%s %s %s', "
		"                regexp_replace(n.nspname, '[\n\r]', ' '), "
		"                regexp_replace(r.relname, '[\n\r]', ' '), "
		"                regexp_replace(auth.rolname, '[\n\r]', ' '))"
		"    from pg_class r "
		"         join pg_namespace n ON n.oid = r.relnamespace "
		"         join pg_roles auth ON auth.oid = r.relowner"

		/* include-only-table */
		"    left join pg_temp.filter_include_only_table inc "
		"           on n.nspname = inc.nspname "
		"          and r.relname = inc.relname "

		"   where r.relkind = 'r' and r.relpersistence = 'p'  "
		"     and n.nspname !~ '^pg_' and n.nspname <> 'information_schema' "
		"     and not exists "
		"         ( "
		"           select c.oid "
		"             from pg_constraint c "
		"            where c.conrelid = r.oid "
		"              and c.contype = 'p' "
		"         ) "

		/* WHERE clause for exclusion filters */
		"     and inc.nspname is null "

		/* avoid pg_class entries which belong to extensions */
		"     and not exists "
		"       ( "
		"         select 1 "
		"           from pg_depend d "
		"          where d.classid = 'pg_class'::regclass "
		"            and d.objid = c.oid "
		"            and d.deptype = 'e' "
		"       ) "

		"order by n.nspname, r.relname"
	},

	{
		SOURCE_FILTER_TYPE_LIST_EXCL,

		"  select r.oid, n.nspname, r.relname, r.reltuples::bigint, "
		"         pg_table_size(r.oid) as bytes, "
		"         pg_size_pretty(pg_table_size(r.oid)), "
		"         false as excludedata, "
		"         format('%s %s %s', "
		"                regexp_replace(n.nspname, '[\n\r]', ' '), "
		"                regexp_replace(r.relname, '[\n\r]', ' '), "
		"                regexp_replace(auth.rolname, '[\n\r]', ' '))"
		"    from pg_class r "
		"         join pg_namespace n ON n.oid = r.relnamespace "
		"         join pg_roles auth ON auth.oid = r.relowner"

		/* exclude-schema */
		"         left join pg_temp.filter_exclude_schema fn "
		"                on n.nspname = fn.nspname "

		/* exclude-table */
		"         left join pg_temp.filter_exclude_table ft "
		"                on n.nspname = ft.nspname "
		"               and r.relname = ft.relname "

		"   where r.relkind = 'r' and r.relpersistence = 'p'  "
		"     and n.nspname !~ '^pg_' and n.nspname <> 'information_schema' "
		"     and not exists "
		"         ( "
		"           select c.oid "
		"             from pg_constraint c "
		"            where c.conrelid = r.oid "
		"              and c.contype = 'p' "
		"         ) "

		/* WHERE clause for exclusion filters */
		"     and (   fn.nspname is not null "
		"          or ft.relname is not null ) "

		/* avoid pg_class entries which belong to extensions */
		"     and not exists "
		"       ( "
		"         select 1 "
		"           from pg_depend d "
		"          where d.classid = 'pg_class'::regclass "
		"            and d.objid = c.oid "
		"            and d.deptype = 'e' "
		"       ) "

		"order by n.nspname, r.relname"
	}
};

/*
 * schema_list_ordinary_tables_without_pk lists all tables that do not have a
 * primary key. This is useful to prepare a migration when some kind of change
 * data capture technique is considered.
 */
bool
schema_list_ordinary_tables_without_pk(PGSQL *pgsql,
									   SourceFilters *filters,
									   SourceTableArray *tableArray)
{
	SourceTableArrayContext context = { { 0 }, tableArray, false };

	log_trace("schema_list_ordinary_tables_without_pk");

	switch (filters->type)
	{
		case SOURCE_FILTER_TYPE_NONE:
		{
			/* skip filters preparing (temp tables) */
			break;
		}

		case SOURCE_FILTER_TYPE_INCL:
		case SOURCE_FILTER_TYPE_EXCL:
		case SOURCE_FILTER_TYPE_LIST_NOT_INCL:
		case SOURCE_FILTER_TYPE_LIST_EXCL:
		{
			if (!prepareFilters(pgsql, filters))
			{
				log_error("Failed to prepare pgcopydb filters, "
						  "see above for details");
				return false;
			}
			break;
		}

		/* SOURCE_FILTER_TYPE_EXCL_INDEX etc */
		default:
		{
			log_error("BUG: schema_list_ordinary_tables_without_pk called with "
					  "filtering type %d",
					  filters->type);
			return false;
		}
	}

	log_debug("listSourceTablesNoPKSQL[%s]", filterTypeToString(filters->type));

	char *sql = listSourceTablesNoPKSQL[filters->type].sql;

	if (!pgsql_execute_with_params(pgsql, sql, 0, NULL, NULL,
								   &context, &getTableArray))
	{
		log_error("Failed to list tables without primary key");
		return false;
	}

	if (!context.parsedOk)
	{
		log_error("Failed to list tables without primary key");
		return false;
	}

	return true;
}


/*
 * For code simplicity the index array is also the SourceFilterType enum value.
 */
struct FilteringQueries listSourceSequencesSQL[] = {
	{
		SOURCE_FILTER_TYPE_NONE,

		"  select c.oid, n.nspname, c.relname, "
		"         format('%s %s %s', "
		"                regexp_replace(n.nspname, '[\n\r]', ' '), "
		"                regexp_replace(c.relname, '[\n\r]', ' '), "
		"                regexp_replace(auth.rolname, '[\n\r]', ' '))"

		"    from pg_catalog.pg_class c "
		"         join pg_catalog.pg_namespace n on c.relnamespace = n.oid "
		"         join pg_roles auth ON auth.oid = c.relowner"

		"   where c.relkind = 'S' and c.relpersistence = 'p' "
		"     and n.nspname !~ '^pg_' and n.nspname <> 'information_schema' "

		/* avoid pg_class entries which belong to extensions */
		"     and not exists "
		"       ( "
		"         select 1 "
		"           from pg_depend d "
		"          where d.classid = 'pg_class'::regclass "
		"            and d.objid = c.oid "
		"            and d.deptype = 'e' "
		"       ) "

		"order by n.nspname, c.relname"
	},

	{
		SOURCE_FILTER_TYPE_INCL,

		"  select s.oid as seqoid, "
		"         sn.nspname, "
		"         s.relname, "
		"         format('%s %s %s', "
		"                regexp_replace(sn.nspname, '[\n\r]', ' '), "
		"                regexp_replace(s.relname, '[\n\r]', ' '), "
		"                regexp_replace(auth.rolname, '[\n\r]', ' '))"

		/*
		 * we don't need dependent table name and column name:
		 *
		 * "         a.adrelid as reloid, "
		 * "         a.adrelid::regclass as relname, "
		 * "         at.attname as colname "
		 */

		"    from pg_class s "
		"         join pg_namespace sn on sn.oid = s.relnamespace "
		"         join pg_roles auth ON auth.oid = s.relowner"
		"         join pg_depend d on d.refobjid = s.oid "
		"         join pg_attrdef a on d.objid = a.oid "
		"         join pg_attribute at "
		"           on at.attrelid = a.adrelid "
		"          and at.attnum = a.adnum "

		/* include-only-table */
		"         join pg_class r on r.oid = a.adrelid "
		"         join pg_namespace rn on rn.oid = r.relnamespace "

		"         join pg_temp.filter_include_only_table inc "
		"           on rn.nspname = inc.nspname "
		"          and r.relname = inc.relname "

		"  where s.relkind = 'S' "
		"    and d.classid = 'pg_attrdef'::regclass "
		"    and d.refclassid = 'pg_class'::regclass "

		/* avoid pg_class entries which belong to extensions */
		"     and not exists "
		"       ( "
		"         select 1 "
		"           from pg_depend d "
		"          where d.classid = 'pg_class'::regclass "
		"            and d.objid = s.oid "
		"            and d.deptype = 'e' "
		"       ) "

		"order by sn.nspname, s.relname"
	},

	{
		SOURCE_FILTER_TYPE_EXCL,


		"  select s.oid as seqoid, "
		"         sn.nspname, "
		"         s.relname, "
		"         format('%s %s %s', "
		"                regexp_replace(sn.nspname, '[\n\r]', ' '), "
		"                regexp_replace(s.relname, '[\n\r]', ' '), "
		"                regexp_replace(auth.rolname, '[\n\r]', ' '))"

		/*
		 * we don't need dependent table name and column name:
		 *
		 * "         a.adrelid as reloid, "
		 * "         a.adrelid::regclass as relname, "
		 * "         at.attname as colname "
		 */

		"    from pg_class s "
		"         join pg_namespace sn on sn.oid = s.relnamespace "
		"         join pg_roles auth ON auth.oid = s.relowner"
		"         join pg_depend d on d.refobjid = s.oid "
		"         join pg_attrdef a on d.objid = a.oid "
		"         join pg_attribute at "
		"           on at.attrelid = a.adrelid "
		"          and at.attnum = a.adnum "

		/* filters are edited in terms of the main relation pg_class entry */
		"         join pg_class r on r.oid = a.adrelid "
		"         join pg_namespace rn on rn.oid = r.relnamespace "

		/* exclude-schema */
		"         left join pg_temp.filter_exclude_schema fn "
		"                on rn.nspname = fn.nspname "

		/* exclude-table */
		"         left join pg_temp.filter_exclude_table ft "
		"                on rn.nspname = ft.nspname "
		"               and r.relname = ft.relname "

		/* exclude-table-data */
		"         left join pg_temp.filter_exclude_table_data ftd "
		"                on rn.nspname = ftd.nspname "
		"               and r.relname = ftd.relname "

		"  where s.relkind = 'S' "
		"    and d.classid = 'pg_attrdef'::regclass "
		"    and d.refclassid = 'pg_class'::regclass "

		/* WHERE clause for exclusion filters */
		"     and fn.nspname is null "
		"     and ft.relname is null "
		"     and ftd.relname is null "

		/* avoid pg_class entries which belong to extensions */
		"     and not exists "
		"       ( "
		"         select 1 "
		"           from pg_depend d "
		"          where d.classid = 'pg_class'::regclass "
		"            and d.objid = s.oid "
		"            and d.deptype = 'e' "
		"       ) "

		"order by sn.nspname, s.relname"
	},

	{
		SOURCE_FILTER_TYPE_LIST_NOT_INCL,

		"  select s.oid as seqoid, "
		"         sn.nspname, "
		"         s.relname, "
		"         format('%s %s %s', "
		"                regexp_replace(sn.nspname, '[\n\r]', ' '), "
		"                regexp_replace(s.relname, '[\n\r]', ' '), "
		"                regexp_replace(auth.rolname, '[\n\r]', ' '))"

		/*
		 * we don't need dependent table name and column name:
		 *
		 * "         a.adrelid as reloid, "
		 * "         a.adrelid::regclass as relname, "
		 * "         at.attname as colname "
		 */

		"    from pg_class s "
		"         join pg_namespace sn on sn.oid = s.relnamespace "
		"         join pg_roles auth ON auth.oid = s.relowner"
		"         join pg_depend d on d.refobjid = s.oid "
		"         join pg_attrdef a on d.objid = a.oid "
		"         join pg_attribute at "
		"           on at.attrelid = a.adrelid "
		"          and at.attnum = a.adnum "

		/* include-only-table */
		"         join pg_class r on r.oid = a.adrelid "
		"         join pg_namespace rn on rn.oid = r.relnamespace "

		"    left join pg_temp.filter_include_only_table inc "
		"           on rn.nspname = inc.nspname "
		"          and r.relname = inc.relname "

		"  where s.relkind = 'S' "
		"    and d.classid = 'pg_attrdef'::regclass "
		"    and d.refclassid = 'pg_class'::regclass "

		/* WHERE clause for exclusion filters */
		"     and inc.relname is null "

		/* avoid pg_class entries which belong to extensions */
		"     and not exists "
		"       ( "
		"         select 1 "
		"           from pg_depend d "
		"          where d.classid = 'pg_class'::regclass "
		"            and d.objid = s.oid "
		"            and d.deptype = 'e' "
		"       ) "

		"order by sn.nspname, s.relname"
	},

	{
		SOURCE_FILTER_TYPE_LIST_EXCL,


		"  select s.oid as seqoid, "
		"         sn.nspname, "
		"         s.relname, "
		"         format('%s %s %s', "
		"                regexp_replace(sn.nspname, '[\n\r]', ' '), "
		"                regexp_replace(s.relname, '[\n\r]', ' '), "
		"                regexp_replace(auth.rolname, '[\n\r]', ' '))"

		/*
		 * we don't need dependent table name and column name:
		 *
		 * "         a.adrelid as reloid, "
		 * "         a.adrelid::regclass as relname, "
		 * "         at.attname as colname "
		 */

		"    from pg_class s "
		"         join pg_namespace sn on sn.oid = s.relnamespace "
		"         join pg_roles auth ON auth.oid = s.relowner"
		"         join pg_depend d on d.refobjid = s.oid "
		"         join pg_attrdef a on d.objid = a.oid "
		"         join pg_attribute at "
		"           on at.attrelid = a.adrelid "
		"          and at.attnum = a.adnum "

		/* filters are edited in terms of the main relation pg_class entry */
		"         join pg_class r on r.oid = a.adrelid "
		"         join pg_namespace rn on rn.oid = r.relnamespace "

		/* exclude-schema */
		"         left join pg_temp.filter_exclude_schema fn "
		"                on rn.nspname = fn.nspname "

		/* exclude-table */
		"         left join pg_temp.filter_exclude_table ft "
		"                on rn.nspname = ft.nspname "
		"               and r.relname = ft.relname "

		"  where s.relkind = 'S' "
		"    and d.classid = 'pg_attrdef'::regclass "
		"    and d.refclassid = 'pg_class'::regclass "

		/* WHERE clause for exclusion filters */
		"     and (   fn.nspname is not null "
		"          or ft.relname is not null) "

		/* avoid pg_class entries which belong to extensions */
		"     and not exists "
		"       ( "
		"         select 1 "
		"           from pg_depend d "
		"          where d.classid = 'pg_class'::regclass "
		"            and d.objid = s.oid "
		"            and d.deptype = 'e' "
		"       ) "

		"order by sn.nspname, s.relname"
	},
};


/*
 * schema_list_sequences grabs the list of sequences from the given source
 * Postgres instance and allocates a SourceSequence array with the result of
 * the query.
 */
bool
schema_list_sequences(PGSQL *pgsql,
					  SourceFilters *filters,
					  SourceSequenceArray *seqArray)
{
	SourceSequenceArrayContext context = { { 0 }, seqArray, false };

	log_trace("schema_list_sequences");

	switch (filters->type)
	{
		case SOURCE_FILTER_TYPE_NONE:
		{
			/* skip filters preparing (temp tables) */
			break;
		}

		case SOURCE_FILTER_TYPE_INCL:
		case SOURCE_FILTER_TYPE_EXCL:
		case SOURCE_FILTER_TYPE_LIST_NOT_INCL:
		case SOURCE_FILTER_TYPE_LIST_EXCL:
		{
			if (!prepareFilters(pgsql, filters))
			{
				log_error("Failed to prepare pgcopydb filters, "
						  "see above for details");
				return false;
			}
			break;
		}

		/* SOURCE_FILTER_TYPE_EXCL_INDEX etc */
		default:
		{
			log_error("BUG: schema_list_sequences called with "
					  "filtering type %d",
					  filters->type);
			return false;
		}
	}

	log_debug("listSourceSequencesSQL[%s]", filterTypeToString(filters->type));

	char *sql = listSourceSequencesSQL[filters->type].sql;

	if (!pgsql_execute_with_params(pgsql, sql, 0, NULL, NULL,
								   &context, &getSequenceArray))
	{
		log_error("Failed to list sequences");
		return false;
	}

	if (!context.parsedOk)
	{
		log_error("Failed to list sequences");
		return false;
	}

	return true;
}


/*
 * schema_get_sequence_value fetches sequence metadata last_value and
 * is_called for the given sequence.
 */
bool
schema_get_sequence_value(PGSQL *pgsql, SourceSequence *seq)
{
	return pgsql_get_sequence(pgsql,
							  seq->nspname,
							  seq->relname,
							  &(seq->lastValue),
							  &(seq->isCalled));
}


/*
 * schema_set_sequence_value calls pg_catalog.setval() on the given sequence.
 */
bool
schema_set_sequence_value(PGSQL *pgsql, SourceSequence *seq)
{
	SingleValueResultContext parseContext = { { 0 }, PGSQL_RESULT_BIGINT, false };
	char *sql = "select pg_catalog.setval(format('%I.%I', $1, $2), $3, $4)";

	int paramCount = 4;
	Oid paramTypes[4] = { TEXTOID, TEXTOID, INT8OID, BOOLOID };
	const char *paramValues[4];

	paramValues[0] = seq->nspname;
	paramValues[1] = seq->relname;
	paramValues[2] = intToString(seq->lastValue).strValue;
	paramValues[3] = seq->isCalled ? "true" : "false";

	if (!pgsql_execute_with_params(pgsql, sql,
								   paramCount, paramTypes, paramValues,
								   &parseContext, &parseSingleValueResult))
	{
		log_error("Failed to set sequence \"%s\".\"%s\" last value to %lld",
				  seq->nspname, seq->relname, (long long) seq->lastValue);
		return false;
	}

	if (!parseContext.parsedOk)
	{
		log_error("Failed to set sequence \"%s\".\"%s\" last value to %lld",
				  seq->nspname, seq->relname, (long long) seq->lastValue);
		return false;
	}

	return true;
}


/*
 * For code simplicity the index array is also the SourceFilterType enum value.
 */
struct FilteringQueries listSourceIndexesSQL[] = {
	{
		SOURCE_FILTER_TYPE_NONE,

		"   select i.oid, n.nspname, i.relname,"
		"          r.oid, rn.nspname, r.relname,"
		"          indisprimary,"
		"          indisunique,"
		"          (select string_agg(attname, ',')"
		"             from pg_attribute"
		"            where attrelid = r.oid"
		"              and array[attnum::integer] <@ indkey::integer[]"
		"          ) as cols,"
		"          pg_get_indexdef(indexrelid),"
		"          c.oid,"
		"          c.conname,"
		"          pg_get_constraintdef(c.oid),"
		"          format('%s %s %s', "
		"                 regexp_replace(n.nspname, '[\n\r]', ' '), "
		"                 regexp_replace(i.relname, '[\n\r]', ' '), "
		"                 regexp_replace(auth.rolname, '[\n\r]', ' '))"

		"     from pg_index x"
		"          join pg_class i ON i.oid = x.indexrelid"
		"          join pg_class r ON r.oid = x.indrelid"
		"          join pg_namespace n ON n.oid = i.relnamespace"
		"          join pg_namespace rn ON rn.oid = r.relnamespace"
		"          join pg_roles auth ON auth.oid = i.relowner"
		"          left join pg_depend d "
		"                 on d.classid = 'pg_class'::regclass"
		"                and d.objid = i.oid"
		"                and d.refclassid = 'pg_constraint'::regclass"
		"                and d.deptype = 'i'"
		"          left join pg_constraint c ON c.oid = d.refobjid"

		"    where r.relkind = 'r' and r.relpersistence = 'p' "
		"      and n.nspname !~ '^pg_' and n.nspname <> 'information_schema'"

		/* avoid pg_class entries which belong to extensions */
		"     and not exists "
		"       ( "
		"         select 1 "
		"           from pg_depend d "
		"          where d.classid = 'pg_class'::regclass "
		"            and d.objid = r.oid "
		"            and d.deptype = 'e' "
		"       ) "

		" order by n.nspname, r.relname"
	},

	{
		SOURCE_FILTER_TYPE_INCL,

		"   select i.oid, n.nspname, i.relname,"
		"          r.oid, rn.nspname, r.relname,"
		"          indisprimary,"
		"          indisunique,"
		"          (select string_agg(attname, ',')"
		"             from pg_attribute"
		"            where attrelid = r.oid"
		"              and array[attnum::integer] <@ indkey::integer[]"
		"          ) as cols,"
		"          pg_get_indexdef(indexrelid),"
		"          c.oid,"
		"          c.conname,"
		"          pg_get_constraintdef(c.oid),"
		"          format('%s %s %s', "
		"                 regexp_replace(n.nspname, '[\n\r]', ' '), "
		"                 regexp_replace(i.relname, '[\n\r]', ' '), "
		"                 regexp_replace(auth.rolname, '[\n\r]', ' '))"
		"     from pg_index x"
		"          join pg_class i ON i.oid = x.indexrelid"
		"          join pg_class r ON r.oid = x.indrelid"
		"          join pg_namespace n ON n.oid = i.relnamespace"
		"          join pg_namespace rn ON rn.oid = r.relnamespace"
		"          join pg_roles auth ON auth.oid = i.relowner"
		"          left join pg_depend d "
		"                 on d.classid = 'pg_class'::regclass"
		"                and d.objid = i.oid"
		"                and d.refclassid = 'pg_constraint'::regclass"
		"                and d.deptype = 'i'"
		"          left join pg_constraint c ON c.oid = d.refobjid"

		/* include-only-table */
		"         join pg_temp.filter_include_only_table inc "
		"           on rn.nspname = inc.nspname "
		"          and r.relname = inc.relname "

		"    where r.relkind = 'r' and r.relpersistence = 'p' "
		"      and n.nspname !~ '^pg_' and n.nspname <> 'information_schema'"

		/* avoid pg_class entries which belong to extensions */
		"     and not exists "
		"       ( "
		"         select 1 "
		"           from pg_depend d "
		"          where d.classid = 'pg_class'::regclass "
		"            and d.objid = r.oid "
		"            and d.deptype = 'e' "
		"       ) "

		" order by n.nspname, r.relname"
	},

	{
		SOURCE_FILTER_TYPE_EXCL,

		"   select i.oid, n.nspname, i.relname,"
		"          r.oid, rn.nspname, r.relname,"
		"          indisprimary,"
		"          indisunique,"
		"          (select string_agg(attname, ',')"
		"             from pg_attribute"
		"            where attrelid = r.oid"
		"              and array[attnum::integer] <@ indkey::integer[]"
		"          ) as cols,"
		"          pg_get_indexdef(indexrelid),"
		"          c.oid,"
		"          c.conname,"
		"          pg_get_constraintdef(c.oid),"
		"          format('%s %s %s', "
		"                 regexp_replace(n.nspname, '[\n\r]', ' '), "
		"                 regexp_replace(i.relname, '[\n\r]', ' '), "
		"                 regexp_replace(auth.rolname, '[\n\r]', ' '))"
		"     from pg_index x"
		"          join pg_class i ON i.oid = x.indexrelid"
		"          join pg_class r ON r.oid = x.indrelid"
		"          join pg_namespace n ON n.oid = i.relnamespace"
		"          join pg_namespace rn ON rn.oid = r.relnamespace"
		"          join pg_roles auth ON auth.oid = i.relowner"
		"          left join pg_depend d "
		"                 on d.classid = 'pg_class'::regclass"
		"                and d.objid = i.oid"
		"                and d.refclassid = 'pg_constraint'::regclass"
		"                and d.deptype = 'i'"
		"          left join pg_constraint c ON c.oid = d.refobjid"

		/* exclude-schema */
		"         left join pg_temp.filter_exclude_schema fn "
		"                on rn.nspname = fn.nspname "

		/* exclude-table */
		"         left join pg_temp.filter_exclude_table ft "
		"                on rn.nspname = ft.nspname "
		"               and r.relname = ft.relname "

		/* exclude-table-data */
		"         left join pg_temp.filter_exclude_table_data ftd "
		"                on rn.nspname = ftd.nspname "
		"               and r.relname = ftd.relname "

		"    where r.relkind = 'r' and r.relpersistence = 'p' "
		"      and n.nspname !~ '^pg_' and n.nspname <> 'information_schema'"

		/* WHERE clause for exclusion filters */
		"     and fn.nspname is null "
		"     and ft.relname is null "
		"     and ftd.relname is null "

		/* avoid pg_class entries which belong to extensions */
		"     and not exists "
		"       ( "
		"         select 1 "
		"           from pg_depend d "
		"          where d.classid = 'pg_class'::regclass "
		"            and d.objid = r.oid "
		"            and d.deptype = 'e' "
		"       ) "

		" order by n.nspname, r.relname"
	},

	{
		SOURCE_FILTER_TYPE_LIST_NOT_INCL,

		"   select i.oid, n.nspname, i.relname,"
		"          r.oid, rn.nspname, r.relname,"
		"          indisprimary,"
		"          indisunique,"
		"          (select string_agg(attname, ',')"
		"             from pg_attribute"
		"            where attrelid = r.oid"
		"              and array[attnum::integer] <@ indkey::integer[]"
		"          ) as cols,"
		"          pg_get_indexdef(indexrelid),"
		"          c.oid,"
		"          c.conname,"
		"          pg_get_constraintdef(c.oid),"
		"          format('%s %s %s', "
		"                 regexp_replace(n.nspname, '[\n\r]', ' '), "
		"                 regexp_replace(i.relname, '[\n\r]', ' '), "
		"                 regexp_replace(auth.rolname, '[\n\r]', ' '))"
		"     from pg_index x"
		"          join pg_class i ON i.oid = x.indexrelid"
		"          join pg_class r ON r.oid = x.indrelid"
		"          join pg_namespace n ON n.oid = i.relnamespace"
		"          join pg_namespace rn ON rn.oid = r.relnamespace"
		"          join pg_roles auth ON auth.oid = i.relowner"
		"          left join pg_depend d "
		"                 on d.classid = 'pg_class'::regclass"
		"                and d.objid = i.oid"
		"                and d.refclassid = 'pg_constraint'::regclass"
		"                and d.deptype = 'i'"
		"          left join pg_constraint c ON c.oid = d.refobjid"

		/* include-only-table */
		"    left join pg_temp.filter_include_only_table inc "
		"           on rn.nspname = inc.nspname "
		"          and r.relname = inc.relname "

		"    where r.relkind = 'r' and r.relpersistence = 'p' "
		"      and n.nspname !~ '^pg_' and n.nspname <> 'information_schema'"

		/* WHERE clause for exclusion filters */
		"     and inc.relname is null "

		/* avoid pg_class entries which belong to extensions */
		"     and not exists "
		"       ( "
		"         select 1 "
		"           from pg_depend d "
		"          where d.classid = 'pg_class'::regclass "
		"            and d.objid = r.oid "
		"            and d.deptype = 'e' "
		"       ) "

		" order by n.nspname, r.relname"
	},

	{
		SOURCE_FILTER_TYPE_LIST_EXCL,

		"   select i.oid, n.nspname, i.relname,"
		"          r.oid, rn.nspname, r.relname,"
		"          indisprimary,"
		"          indisunique,"
		"          (select string_agg(attname, ',')"
		"             from pg_attribute"
		"            where attrelid = r.oid"
		"              and array[attnum::integer] <@ indkey::integer[]"
		"          ) as cols,"
		"          pg_get_indexdef(indexrelid),"
		"          c.oid,"
		"          c.conname,"
		"          pg_get_constraintdef(c.oid),"
		"          format('%s %s %s', "
		"                 regexp_replace(n.nspname, '[\n\r]', ' '), "
		"                 regexp_replace(i.relname, '[\n\r]', ' '), "
		"                 regexp_replace(auth.rolname, '[\n\r]', ' '))"
		"     from pg_index x"
		"          join pg_class i ON i.oid = x.indexrelid"
		"          join pg_class r ON r.oid = x.indrelid"
		"          join pg_namespace n ON n.oid = i.relnamespace"
		"          join pg_namespace rn ON rn.oid = r.relnamespace"
		"          join pg_roles auth ON auth.oid = i.relowner"
		"          left join pg_depend d "
		"                 on d.classid = 'pg_class'::regclass"
		"                and d.objid = i.oid"
		"                and d.refclassid = 'pg_constraint'::regclass"
		"                and d.deptype = 'i'"
		"          left join pg_constraint c ON c.oid = d.refobjid"

		/* exclude-schema */
		"         left join pg_temp.filter_exclude_schema fn "
		"                on rn.nspname = fn.nspname "

		/* exclude-table */
		"         left join pg_temp.filter_exclude_table ft "
		"                on rn.nspname = ft.nspname "
		"               and r.relname = ft.relname "

		"    where r.relkind = 'r' and r.relpersistence = 'p' "
		"      and n.nspname !~ '^pg_' and n.nspname <> 'information_schema'"

		/* WHERE clause for exclusion filters */
		"     and (   fn.nspname is not null "
		"          or ft.relname is not null ) "

		/* avoid pg_class entries which belong to extensions */
		"     and not exists "
		"       ( "
		"         select 1 "
		"           from pg_depend d "
		"          where d.classid = 'pg_class'::regclass "
		"            and d.objid = r.oid "
		"            and d.deptype = 'e' "
		"       ) "

		" order by n.nspname, r.relname"
	},

	{
		SOURCE_FILTER_TYPE_EXCL_INDEX,

		"   select i.oid, n.nspname, i.relname,"
		"          r.oid, rn.nspname, r.relname,"
		"          indisprimary,"
		"          indisunique,"
		"          (select string_agg(attname, ',')"
		"             from pg_attribute"
		"            where attrelid = r.oid"
		"              and array[attnum::integer] <@ indkey::integer[]"
		"          ) as cols,"
		"          pg_get_indexdef(indexrelid),"
		"          c.oid,"
		"          c.conname,"
		"          pg_get_constraintdef(c.oid),"
		"          format('%s %s %s', "
		"                 regexp_replace(n.nspname, '[\n\r]', ' '), "
		"                 regexp_replace(i.relname, '[\n\r]', ' '), "
		"                 regexp_replace(auth.rolname, '[\n\r]', ' '))"
		"     from pg_index x"
		"          join pg_class i ON i.oid = x.indexrelid"
		"          join pg_class r ON r.oid = x.indrelid"
		"          join pg_namespace n ON n.oid = i.relnamespace"
		"          join pg_namespace rn ON rn.oid = r.relnamespace"
		"          join pg_roles auth ON auth.oid = i.relowner"
		"          left join pg_depend d "
		"                 on d.classid = 'pg_class'::regclass"
		"                and d.objid = i.oid"
		"                and d.refclassid = 'pg_constraint'::regclass"
		"                and d.deptype = 'i'"
		"          left join pg_constraint c ON c.oid = d.refobjid"

		/* exclude-index */
		"          left join filter_exclude_index ft "
		"                 on n.nspname = ft.nspname "
		"                and i.relname = ft.relname "

		"    where r.relkind = 'r' and r.relpersistence = 'p' "
		"      and n.nspname !~ '^pg_' and n.nspname <> 'information_schema'"

		/* WHERE clause for exclusion filters */
		"     and fn.nspname is null "

		/* avoid pg_class entries which belong to extensions */
		"     and not exists "
		"       ( "
		"         select 1 "
		"           from pg_depend d "
		"          where d.classid = 'pg_class'::regclass "
		"            and d.objid = r.oid "
		"            and d.deptype = 'e' "
		"       ) "

		" order by n.nspname, r.relname"
	},

	{
		SOURCE_FILTER_TYPE_LIST_EXCL_INDEX,

		"   select i.oid, n.nspname, i.relname,"
		"          r.oid, rn.nspname, r.relname,"
		"          indisprimary,"
		"          indisunique,"
		"          (select string_agg(attname, ',')"
		"             from pg_attribute"
		"            where attrelid = r.oid"
		"              and array[attnum::integer] <@ indkey::integer[]"
		"          ) as cols,"
		"          pg_get_indexdef(indexrelid),"
		"          c.oid,"
		"          c.conname,"
		"          pg_get_constraintdef(c.oid),"
		"          format('%s %s %s', "
		"                 regexp_replace(n.nspname, '[\n\r]', ' '), "
		"                 regexp_replace(i.relname, '[\n\r]', ' '), "
		"                 regexp_replace(auth.rolname, '[\n\r]', ' '))"
		"     from pg_index x"
		"          join pg_class i ON i.oid = x.indexrelid"
		"          join pg_class r ON r.oid = x.indrelid"
		"          join pg_namespace n ON n.oid = i.relnamespace"
		"          join pg_namespace rn ON rn.oid = r.relnamespace"
		"          join pg_roles auth ON auth.oid = i.relowner"
		"          left join pg_depend d "
		"                 on d.classid = 'pg_class'::regclass"
		"                and d.objid = i.oid"
		"                and d.refclassid = 'pg_constraint'::regclass"
		"                and d.deptype = 'i'"
		"          left join pg_constraint c ON c.oid = d.refobjid"

		/* list only exclude-index */
		"               join filter_exclude_index ft "
		"                 on n.nspname = ft.nspname "
		"                and i.relname = ft.relname "

		"    where r.relkind = 'r' and r.relpersistence = 'p' "
		"      and n.nspname !~ '^pg_' and n.nspname <> 'information_schema'"

		/* avoid pg_class entries which belong to extensions */
		"     and not exists "
		"       ( "
		"         select 1 "
		"           from pg_depend d "
		"          where d.classid = 'pg_class'::regclass "
		"            and d.objid = r.oid "
		"            and d.deptype = 'e' "
		"       ) "

		" order by n.nspname, r.relname"
	}
};


/*
 * schema_list_all_indexes grabs the list of indexes from the given source
 * Postgres instance and allocates a SourceIndex array with the result of the
 * query.
 */
bool
schema_list_all_indexes(PGSQL *pgsql,
						SourceFilters *filters,
						SourceIndexArray *indexArray)
{
	SourceIndexArrayContext context = { { 0 }, indexArray, false };

	log_trace("schema_list_all_indexes");

	if (filters->type != SOURCE_FILTER_TYPE_NONE)
	{
		if (!prepareFilters(pgsql, filters))
		{
			log_error("Failed to prepare pgcopydb filters, "
					  "see above for details");
			return false;
		}
	}

	log_debug("listSourceIndexesSQL[%s]", filterTypeToString(filters->type));

	char *sql = listSourceIndexesSQL[filters->type].sql;

	if (!pgsql_execute_with_params(pgsql, sql, 0, NULL, NULL,
								   &context, &getIndexArray))
	{
		log_error("Failed to list all indexes");
		return false;
	}

	if (!context.parsedOk)
	{
		log_error("Failed to list all indexes");
		return false;
	}

	return true;
}


/*
 * schema_list_all_indexes grabs the list of indexes from the given source
 * Postgres instance and allocates a SourceIndex array with the result of the
 * query.
 */
bool
schema_list_table_indexes(PGSQL *pgsql,
						  const char *schemaName,
						  const char *tableName,
						  SourceIndexArray *indexArray)
{
	SourceIndexArrayContext context = { { 0 }, indexArray, false };

	char *sql =
		"   select i.oid, n.nspname, i.relname,"
		"          r.oid, rn.nspname, r.relname,"
		"          indisprimary,"
		"          indisunique,"
		"          (select string_agg(attname, ',')"
		"             from pg_attribute"
		"            where attrelid = r.oid"
		"              and array[attnum::integer] <@ indkey::integer[]"
		"          ) as cols,"
		"          pg_get_indexdef(indexrelid),"
		"          c.oid,"
		"          c.conname,"
		"          pg_get_constraintdef(c.oid),"
		"          format('%s %s %s', "
		"                 regexp_replace(n.nspname, '[\n\r]', ' '), "
		"                 regexp_replace(i.relname, '[\n\r]', ' '), "
		"                 regexp_replace(auth.rolname, '[\n\r]', ' '))"
		"     from pg_index x"
		"          join pg_class i ON i.oid = x.indexrelid"
		"          join pg_class r ON r.oid = x.indrelid"
		"          join pg_namespace n ON n.oid = i.relnamespace"
		"          join pg_namespace rn ON rn.oid = r.relnamespace"
		"          join pg_roles auth ON auth.oid = i.relowner"
		"          left join pg_depend d "
		"                 on d.classid = 'pg_class'::regclass"
		"                and d.objid = i.oid"
		"                and d.refclassid = 'pg_constraint'::regclass"
		"                and d.deptype = 'i'"
		"          left join pg_constraint c ON c.oid = d.refobjid"
		"    where r.relkind = 'r' and r.relpersistence = 'p' "
		"      and n.nspname !~ '^pg_' and n.nspname <> 'information_schema'"
		"      and rn.nspname = $1 and r.relname = $2"
		" order by n.nspname, r.relname";

	int paramCount = 2;
	Oid paramTypes[2] = { TEXTOID, TEXTOID };
	const char *paramValues[2] = { schemaName, tableName };

	log_trace("schema_list_table_indexes");

	if (!pgsql_execute_with_params(pgsql, sql,
								   paramCount, paramTypes, paramValues,
								   &context, &getIndexArray))
	{
		log_error("Failed to list all indexes for table \"%s\".\"%s\"",
				  schemaName, tableName);
		return false;
	}

	if (!context.parsedOk)
	{
		log_error("Failed to list all indexes for table \"%s\".\"%s\"",
				  schemaName, tableName);
		return false;
	}

	return true;
}


/*
 * For code simplicity the index array is also the SourceFilterType enum value.
 */
struct FilteringQueries listSourceDependSQL[] = {
	{
		SOURCE_FILTER_TYPE_NONE, ""
	},

	{
		SOURCE_FILTER_TYPE_INCL,

		PG_DEPEND_SQL
		"  SELECT n.nspname, c.relname, "
		"         refclassid, refobjid, classid, objid, "
		"         deptype, type, identity "
		"    FROM unconcat "

		/* include-only-table */
		"         join pg_class c "
		"           on unconcat.refclassid = 'pg_class'::regclass "
		"          and unconcat.refobjid = c.oid "

		"         join pg_catalog.pg_namespace n on c.relnamespace = n.oid "

		"         join pg_temp.filter_include_only_table inc "
		"           on n.nspname = inc.nspname "
		"          and c.relname = inc.relname "

		"         , pg_identify_object(classid, objid, objsubid) "

		"   WHERE NOT (refclassid = classid AND refobjid = objid) "
		"      and n.nspname !~ '^pg_' and n.nspname <> 'information_schema'"
		"      and type not in ('toast table column', 'default value') "

		/* remove duplicates due to multiple refobjsubid / objsubid */
		"GROUP BY n.nspname, c.relname, "
		"         refclassid, refobjid, classid, objid, deptype, type, identity"
	},

	{
		SOURCE_FILTER_TYPE_EXCL,

		PG_DEPEND_SQL
		"  SELECT n.nspname, relname, "
		"         refclassid, refobjid, classid, objid, "
		"         deptype, type, identity "

		"    FROM pg_namespace n "

		/* exclude-schema */
		"         join pg_temp.filter_exclude_schema fn "
		"           on n.nspname = fn.nspname "

		"         left join unconcat "
		"           on unconcat.refclassid = 'pg_namespace'::regclass "
		"          and unconcat.refobjid = n.oid "

		"         left join pg_class c "
		"           on unconcat.classid = 'pg_class'::regclass "
		"          and unconcat.objid = c.oid "

		"         , pg_identify_object(classid, objid, objsubid) "

		/* remove duplicates due to multiple refobjsubid / objsubid */
		"GROUP BY n.nspname, c.relname, "
		"         refclassid, refobjid, classid, objid, deptype, type, identity"

		" UNION ALL "
		" ( "
		"  SELECT n.nspname, null as relname, "
		"         null as refclassid, null as refobjid, "
		"         'pg_namespace'::regclass::oid as classid, n.oid as objid, "
		"         null as deptype, type, identity "

		"    FROM pg_namespace n "

		/* exclude-schema */
		"         join pg_temp.filter_exclude_schema fn "
		"           on n.nspname = fn.nspname "

		"         , pg_identify_object('pg_namespace'::regclass, n.oid, 0) "
		" ) "

		" UNION ALL "
		" ( "

		"  SELECT cn.nspname, c.relname, "
		"         refclassid, refobjid, classid, objid, "
		"         deptype, type, identity "
		"    FROM unconcat "

		"         left join pg_class c "
		"           on unconcat.refclassid = 'pg_class'::regclass "
		"          and unconcat.refobjid = c.oid "

		"         left join pg_catalog.pg_namespace cn "
		"           on c.relnamespace = cn.oid "

		/* exclude-schema */
		"         left join pg_temp.filter_exclude_schema fn "
		"                on cn.nspname = fn.nspname "

		/* exclude-table */
		"         left join pg_temp.filter_exclude_table ft "
		"                on cn.nspname = ft.nspname "
		"               and c.relname = ft.relname "

		/* exclude-table-data */
		"         left join pg_temp.filter_exclude_table_data ftd "
		"                on cn.nspname = ftd.nspname "
		"               and c.relname = ftd.relname "

		"         , pg_identify_object(classid, objid, objsubid) "

		"   WHERE NOT (refclassid = classid AND refobjid = objid) "
		"      and cn.nspname !~ '^pg_' and cn.nspname <> 'information_schema'"
		"      and type not in ('toast table column', 'default value') "

		/* WHERE clause for exclusion filters */
		"     and fn.nspname is null "
		"     and ft.relname is null "
		"     and ftd.relname is null "

		/* remove duplicates due to multiple refobjsubid / objsubid */
		"GROUP BY cn.nspname, c.relname, "
		"         refclassid, refobjid, classid, objid, deptype, type, identity"

		" ) "
	},

	{
		SOURCE_FILTER_TYPE_LIST_NOT_INCL,

		PG_DEPEND_SQL
		"  SELECT n.nspname, c.relname, "
		"         refclassid, refobjid, classid, objid, "
		"         deptype, type, identity "
		"    FROM unconcat "

		"         join pg_class c "
		"           on unconcat.refclassid = 'pg_class'::regclass "
		"          and unconcat.refobjid = c.oid "

		"         join pg_catalog.pg_namespace n on c.relnamespace = n.oid "

		/* include-only-table */
		"    left join pg_temp.filter_include_only_table inc "
		"           on n.nspname = inc.nspname "
		"          and c.relname = inc.relname "

		"         , pg_identify_object(classid, objid, objsubid) "

		"   WHERE NOT (refclassid = classid AND refobjid = objid) "
		"      and n.nspname !~ '^pg_' and n.nspname <> 'information_schema'"
		"      and type not in ('toast table column', 'default value') "

		/* WHERE clause for exclusion filters */
		"     and inc.nspname is null "

		/* remove duplicates due to multiple refobjsubid / objsubid */
		"GROUP BY n.nspname, c.relname, "
		"         refclassid, refobjid, classid, objid, deptype, type, identity"
	},

	{
		SOURCE_FILTER_TYPE_LIST_EXCL,

		PG_DEPEND_SQL
		"  SELECT n.nspname, relname, "
		"         refclassid, refobjid, classid, objid, "
		"         deptype, type, identity "

		"    FROM pg_namespace n "

		/* exclude-schema */
		"         join pg_temp.filter_exclude_schema fn "
		"           on n.nspname = fn.nspname "

		"         left join unconcat "
		"           on unconcat.refclassid = 'pg_namespace'::regclass "
		"          and unconcat.refobjid = n.oid "

		"         left join pg_class c "
		"           on unconcat.classid = 'pg_class'::regclass "
		"          and unconcat.objid = c.oid "

		"         , pg_identify_object(classid, objid, objsubid) "

		/* remove duplicates due to multiple refobjsubid / objsubid */
		"GROUP BY n.nspname, c.relname, "
		"         refclassid, refobjid, classid, objid, deptype, type, identity"

		" UNION ALL "
		" ( "
		"  SELECT n.nspname, null as relname, "
		"         null as refclassid, null as refobjid, "
		"         'pg_namespace'::regclass::oid as classid, n.oid as objid, "
		"         null as deptype, type, identity "

		"    FROM pg_namespace n "

		/* exclude-schema */
		"         join pg_temp.filter_exclude_schema fn "
		"           on n.nspname = fn.nspname "

		"         , pg_identify_object('pg_namespace'::regclass, n.oid, 0) "
		" ) "

		" UNION ALL "
		" ( "

		"  SELECT n.nspname, c.relname, "
		"         refclassid, refobjid, classid, objid, "
		"         deptype, type, identity "
		"    FROM unconcat "

		"         join pg_class c "
		"           on unconcat.refclassid = 'pg_class'::regclass "
		"          and unconcat.refobjid = c.oid "

		"         join pg_catalog.pg_namespace n "
		"           on c.relnamespace = n.oid "

		/* exclude-schema */
		"         left join pg_temp.filter_exclude_schema fn "
		"                on n.nspname = fn.nspname "

		/* exclude-table */
		"         left join pg_temp.filter_exclude_table ft "
		"                on n.nspname = ft.nspname "
		"               and c.relname = ft.relname "

		"         , pg_identify_object(classid, objid, objsubid) "

		"   WHERE NOT (refclassid = classid AND refobjid = objid) "
		"      and n.nspname !~ '^pg_' and n.nspname <> 'information_schema'"
		"      and type not in ('toast table column', 'default value') "

		/* WHERE clause for exclusion filters */
		"     and (   fn.nspname is not null "
		"          or ft.relname is not null ) "

		/* remove duplicates due to multiple refobjsubid / objsubid */
		"GROUP BY n.nspname, c.relname, "
		"         refclassid, refobjid, classid, objid, deptype, type, identity"

		" ) "
	}
};


/*
 * schema_list_pg_depend recursively walks the pg_catalog.pg_depend view and
 * builds the list of objects that depend on tables that are filtered-out from
 * our operations.
 */
bool
schema_list_pg_depend(PGSQL *pgsql,
					  SourceFilters *filters,
					  SourceDependArray *dependArray)
{
	SourceDependArrayContext context = { { 0 }, dependArray, false };

	log_trace("schema_list_pg_depend");

	switch (filters->type)
	{
		case SOURCE_FILTER_TYPE_INCL:
		case SOURCE_FILTER_TYPE_EXCL:
		case SOURCE_FILTER_TYPE_LIST_NOT_INCL:
		case SOURCE_FILTER_TYPE_LIST_EXCL:
		{
			if (!prepareFilters(pgsql, filters))
			{
				log_error("Failed to prepare pgcopydb filters, "
						  "see above for details");
				return false;
			}
			break;
		}

		/* SOURCE_FILTER_TYPE_EXCL_INDEX etc */
		default:
		{
			log_error("BUG: schema_list_ordinary_tables called with "
					  "filtering type %d",
					  filters->type);
			return false;
		}
	}

	log_debug("listSourceDependSQL[%s]", filterTypeToString(filters->type));

	char *sql = listSourceDependSQL[filters->type].sql;

	if (!pgsql_execute_with_params(pgsql, sql, 0, NULL, NULL,
								   &context, &getDependArray))
	{
		log_error("Failed to list table dependencies");
		return false;
	}

	if (!context.parsedOk)
	{
		log_error("Failed to list table dependencies");
		return false;
	}

	return true;
}


/*
 * schema_list_partitions prepares the list of partitions that we can drive
 * from our parameters: table size, --split-tables-larger-than.
 */
bool
schema_list_partitions(PGSQL *pgsql, SourceTable *table, uint64_t partSize)
{
	/* no partKey, no partitions, done. */
	if (IS_EMPTY_STRING_BUFFER(table->partKey))
	{
		table->partsArray.count = 0;
		return true;
	}

	/* when partSize is zero, just don't partition the COPY */
	if (partSize == 0)
	{
		table->partsArray.count = 0;
		return true;
	}

	SourcePartitionContext parseContext = { { 0 }, table, false };

	char *sqlTemplate =
		" with "
		" t(min, max, parts) as "
		" ( "
		"   select min(\"%s\"), max(\"%s\"), "
		"          ceil(pg_table_size('%s.%s')::float / $1) "
		"     from \"%s\".\"%s\""
		"  ), "
		"  ranges(n, parts, a, b) as "
		"  ( "
		"    select n, "
		"           parts, "
		"           x as a, "
		"           coalesce((lead(x, 1) over(order by n)) - 1, max) as b "
		"      from t, "
		"           generate_series(min, max, ((max-min+1)/parts)::integer + 1) "
		"           with ordinality as s(x, n) "
		"  ) "
		" "
		"  select n, parts, a, b, b-a+1 as count "
		"    from ranges "
		"order by n";

	char sql[BUFSIZE] = { 0 };

	sformat(sql, sizeof(sql), sqlTemplate,
			table->partKey, table->partKey,
			table->nspname, table->relname,
			table->nspname, table->relname);

	int paramCount = 1;
	Oid paramTypes[1] = { INT8OID };
	const char *paramValues[1];

	paramValues[0] = intToString(partSize).strValue;

	if (!pgsql_execute_with_params(pgsql, sql,
								   paramCount, paramTypes, paramValues,
								   &parseContext, &getPartitionList))
	{
		log_error("Failed to compute partition list for table \"%s\".\"%s\"",
				  table->nspname, table->relname);
		return false;
	}

	if (!parseContext.parsedOk)
	{
		log_error("Failed to list table COPY partition list");
		return false;
	}

	return true;
}


/*
 * prepareFilters prepares the temporary tables that are needed on the Postgres
 * session where we want to implement a catalog query with filtering. The
 * filtering rules are then uploaded in those temp tables, and the filtering is
 * implemented with SQL joins.
 */
static bool
prepareFilters(PGSQL *pgsql, SourceFilters *filters)
{
	/*
	 * Temporary tables only are available within a session, so we need a
	 * multi-statement connection here.
	 */
	if (pgsql->connection == NULL)
	{
		/* open a multi-statements connection then */
		pgsql->connectionStatementType = PGSQL_CONNECTION_MULTI_STATEMENT;
	}
	else if (pgsql->connectionStatementType != PGSQL_CONNECTION_MULTI_STATEMENT)
	{
		log_error("BUG: calling prepareFilters with a "
				  "non PGSQL_CONNECTION_MULTI_STATEMENT connection");
		pgsql_finish(pgsql);
		return false;
	}

	/* if the filters have already been prepared, we're good */
	if (filters->prepared)
	{
		return true;
	}

	/*
	 * First, create the temp tables.
	 */
	char *tempTables[] = {
		"create temp table filter_exclude_schema(nspname name)",
		"create temp table filter_include_only_table(nspname name, relname name)",
		"create temp table filter_exclude_table(nspname name, relname name)",
		"create temp table filter_exclude_table_data(nspname name, relname name)",
		"create temp table filter_exclude_index(nspname name, relname name)",
		NULL
	};

	for (int i = 0; tempTables[i] != NULL; i++)
	{
		if (!pgsql_execute(pgsql, tempTables[i]))
		{
			/* errors have already been logged */
			return false;
		}
	}

	/*
	 * Now, fill-in the temp tables with the data that we have.
	 */
	if (!prepareFilterCopyExcludeSchema(pgsql, filters))
	{
		/* errors have already been logged */
		return false;
	}

	struct name_list_pair
	{
		char *name;
		SourceFilterTableList *list;
	};

	struct name_list_pair nameListPair[] = {
		{ "filter_include_only_table", &(filters->includeOnlyTableList) },
		{ "filter_exclude_table", &(filters->excludeTableList) },
		{ "filter_exclude_table_data", &(filters->excludeTableDataList) },
		{ "filter_exclude_index", &(filters->excludeIndexList) },
		{ NULL, NULL },
	};

	for (int i = 0; nameListPair[i].name != NULL; i++)
	{
		if (!prepareFilterCopyTableList(pgsql,
										nameListPair[i].list,
										nameListPair[i].name))
		{
			/* errors have already been logged */
			return false;
		}
	}

	/* mark the filters as prepared already */
	filters->prepared = true;

	return true;
}


/*
 * prepareFilterCopyExcludeSchema sends a COPY from STDIN query and then
 * uploads the local filters that we have in the pg_temp.filter_exclude_schema
 * table.
 */
static bool
prepareFilterCopyExcludeSchema(PGSQL *pgsql, SourceFilters *filters)
{
	char *qname = "\"pg_temp\".\"filter_exclude_schema\"";

	if (!pg_copy_from_stdin(pgsql, qname))
	{
		/* errors have already been logged */
		return false;
	}

	for (int i = 0; i < filters->excludeSchemaList.count; i++)
	{
		char *nspname = filters->excludeSchemaList.array[i].nspname;

		if (!pg_copy_row_from_stdin(pgsql, "s", nspname))
		{
			/* errors have already been logged */
			return false;
		}
	}

	if (!pg_copy_end(pgsql))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * prepareFilterCopyTableList sends a COPY from STDIN query and then uploads
 * the local filters that we have in the given target table.
 */
static bool
prepareFilterCopyTableList(PGSQL *pgsql,
						   SourceFilterTableList *tableList,
						   const char *temp_table_name)
{
	char qname[BUFSIZE] = { 0 };

	sformat(qname, sizeof(qname), "\"pg_temp\".\"%s\"", temp_table_name);

	if (!pg_copy_from_stdin(pgsql, qname))
	{
		/* errors have already been logged */
		return false;
	}

	for (int i = 0; i < tableList->count; i++)
	{
		char *nspname = tableList->array[i].nspname;
		char *relname = tableList->array[i].relname;

		if (!pg_copy_row_from_stdin(pgsql, "ss", nspname, relname))
		{
			/* errors have already been logged */
			return false;
		}
	}

	if (!pg_copy_end(pgsql))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * getSchemaList loops over the SQL result for the schema array query and
 * allocates an array of schemas then populates it with the query result.
 */
static void
getSchemaList(void *ctx, PGresult *result)
{
	SourceSchemaArrayContext *context = (SourceSchemaArrayContext *) ctx;
	int nTuples = PQntuples(result);

	log_debug("getSchemaList: %d", nTuples);

	if (PQnfields(result) != 3)
	{
		log_error("Query returned %d columns, expected 3", PQnfields(result));
		context->parsedOk = false;
		return;
	}

	/* we're not supposed to re-cycle arrays here */
	if (context->schemaArray->array != NULL)
	{
		/* issue a warning but let's try anyway */
		log_warn("BUG? context's array is not null in getSchemaList");

		free(context->schemaArray->array);
		context->schemaArray->array = NULL;
	}

	context->schemaArray->count = nTuples;
	context->schemaArray->array =
		(SourceSchema *) calloc(nTuples, sizeof(SourceSchema));

	if (context->schemaArray->array == NULL)
	{
		log_fatal(ALLOCATION_FAILED_ERROR);
		return;
	}

	int errors = 0;

	for (int rowNumber = 0; rowNumber < nTuples; rowNumber++)
	{
		SourceSchema *schema = &(context->schemaArray->array[rowNumber]);

		/* 1. oid */
		char *value = PQgetvalue(result, rowNumber, 0);

		if (!stringToUInt32(value, &(schema->oid)) || schema->oid == 0)
		{
			log_error("Invalid OID \"%s\"", value);
			++errors;
		}

		/* 2. nspname */
		value = PQgetvalue(result, rowNumber, 1);
		int length = strlcpy(schema->nspname, value, NAMEDATALEN);

		if (length >= NAMEDATALEN)
		{
			log_error("Extension name \"%s\" is %d bytes long, "
					  "the maximum expected is %d (NAMEDATALEN - 1)",
					  value, length, NAMEDATALEN - 1);
			++errors;
		}

		/* 3. restoreListName */
		value = PQgetvalue(result, rowNumber, 2);
		length = strlcpy(schema->restoreListName, value, RESTORE_LIST_NAMEDATALEN);

		if (length >= RESTORE_LIST_NAMEDATALEN)
		{
			log_error("Table restore list name \"%s\" is %d bytes long, "
					  "the maximum expected is %d (RESTORE_LIST_NAMEDATALEN - 1)",
					  value, length, RESTORE_LIST_NAMEDATALEN - 1);
			++errors;
		}
	}

	context->parsedOk = errors == 0;
}


/*
 * getExtensionList loops over the SQL result for the extension array query and
 * allocates an array of extensions then populates it with the query result.
 */
static void
getExtensionList(void *ctx, PGresult *result)
{
	SourceExtensionArrayContext *context = (SourceExtensionArrayContext *) ctx;
	int nTuples = PQntuples(result);

	log_debug("getExtensionList: %d", nTuples);

	if (PQnfields(result) != 10)
	{
		log_error("Query returned %d columns, expected 10", PQnfields(result));
		context->parsedOk = false;
		return;
	}

	/* we're not supposed to re-cycle arrays here */
	if (context->extensionArray->array != NULL)
	{
		/* issue a warning but let's try anyway */
		log_warn("BUG? context's array is not null in getExtensionList");

		free(context->extensionArray->array);
		context->extensionArray->array = NULL;
	}

	context->extensionArray->count = 0;
	context->extensionArray->array =
		(SourceExtension *) calloc(nTuples, sizeof(SourceExtension));

	if (context->extensionArray->array == NULL)
	{
		log_fatal(ALLOCATION_FAILED_ERROR);
		return;
	}

	bool parsedOk = true;

	int extArrayIndex = 0;
	SourceExtension *extension = NULL;

	for (int rowNumber = 0; rowNumber < nTuples; rowNumber++)
	{
		SourceExtension rowExtension = { 0 };
		int confIndex = 0;

		parsedOk = parsedOk &&
				   parseCurrentExtension(result, rowNumber, &rowExtension, &confIndex);

		log_trace("getExtensionList: %s [%d/%d]",
				  rowExtension.extname,
				  confIndex,
				  rowExtension.config.count);

		/*
		 * Only the first extension of a series gets into the extension list.
		 *
		 * Each extension has an array of extconfig (pg_class oids) and an
		 * array of extcondition (WHERE clauses, as text) of the same
		 * dimensions.
		 *
		 * The arrays may be empty, in which case confIndex == 0, and we can
		 * skip the extension configuration parts.
		 *
		 * The arrays may contain a single entry, in which case parsing the
		 * current row is self-contained.
		 *
		 * The arrays may contain 2 or more values, in which case the first row
		 * we read in the loop is where we build the SourceExtension structure
		 * instance, and then the next rows of the SQL query have the same
		 * first columns values and vary only in their extconfig/extcondition
		 * columns. The arrays have been UNNESTed, so each row contains the
		 * next value from the array.
		 */
		if (confIndex == 0 || confIndex == 1)
		{
			/* copy the current rowExtension into the target array entry */
			extension = &(context->extensionArray->array[extArrayIndex++]);
			*extension = rowExtension;

			/* update the extension array count too, not just the index */
			context->extensionArray->count++;
		}

		/* now loop over extension configuration, if any */
		if (extension->config.count > 0)
		{
			/* SQL arrays indexes start at 1, C arrays index start at 0 */
			if (confIndex == 1)
			{
				extension->config.array =
					(SourceExtensionConfig *)
					calloc(extension->config.count,
						   sizeof(SourceExtensionConfig));

				if (extension->config.array == NULL)
				{
					log_fatal(ALLOCATION_FAILED_ERROR);
					parsedOk = false;
					return;
				}
			}

			/* SQL arrays indexes start at 1, C arrays index start at 0 */
			SourceExtensionConfig *extConfig =
				&(extension->config.array[confIndex - 1]);

			parsedOk = parsedOk &&
					   parseCurrentExtensionConfig(result, rowNumber, extConfig);
		}
	}

	if (!parsedOk)
	{
		free(context->extensionArray->array);
		context->extensionArray->array = NULL;
	}

	context->parsedOk = parsedOk;
}


/*
 * parseCurrentSourceTable parses a single row of the extension listing query
 * result.
 */
static bool
parseCurrentExtension(PGresult *result,
					  int rowNumber,
					  SourceExtension *extension,
					  int *confIndex)
{
	int errors = 0;

	/* 1. oid */
	char *value = PQgetvalue(result, rowNumber, 0);

	if (!stringToUInt32(value, &(extension->oid)) || extension->oid == 0)
	{
		log_error("Invalid OID \"%s\"", value);
		++errors;
	}

	/* 2. extname */
	value = PQgetvalue(result, rowNumber, 1);
	int length = strlcpy(extension->extname, value, NAMEDATALEN);

	if (length >= NAMEDATALEN)
	{
		log_error("Extension name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (NAMEDATALEN - 1)",
				  value, length, NAMEDATALEN - 1);
		++errors;
	}

	/* 3. extnamespace */
	value = PQgetvalue(result, rowNumber, 2);
	length = strlcpy(extension->extnamespace, value, NAMEDATALEN);

	if (length >= NAMEDATALEN)
	{
		log_error("Extension extnamespace \"%s\" is %d bytes long, "
				  "the maximum expected is %d (NAMEDATALEN - 1)",
				  value, length, NAMEDATALEN - 1);
		++errors;
	}

	/* 4. extrelocatable */
	value = PQgetvalue(result, rowNumber, 3);
	extension->extrelocatable = (*value) == 't';

	/* 5. array_length(extconfig), or NULL */
	if (PQgetisnull(result, rowNumber, 4))
	{
		extension->config.count = 0;
	}
	else
	{
		value = PQgetvalue(result, rowNumber, 4);

		if (!stringToInt(value, &(extension->config.count)))
		{
			log_error("Invalid extension configuration count \"%s\"", value);
			++errors;
		}
	}

	/* 6. n (position over count), or NULL */
	if (PQgetisnull(result, rowNumber, 5))
	{
		*confIndex = 0;
	}
	else
	{
		value = PQgetvalue(result, rowNumber, 5);

		if (!stringToInt(value, confIndex))
		{
			log_error("Invalid extension configuration index \"%s\"", value);
			++errors;
		}
	}

	return errors == 0;
}


/*
 * parseCurrentExtensionConfig parses a single row of the extension listing
 * query and adds the extconfig and extcondition columns to the given array
 * entry of SourceExtensionConfig.
 */
static bool
parseCurrentExtensionConfig(PGresult *result,
							int rowNumber,
							SourceExtensionConfig *extConfig)
{
	int errors = 0;

	/* 7. extconfig (pg_class oid) */
	char *value = PQgetvalue(result, rowNumber, 6);

	if (!stringToUInt32(value, &(extConfig->oid)))
	{
		log_error("Invalid extension configuration OID \"%s\"", value);
		++errors;
	}

	/* 8. n.nspname */
	value = PQgetvalue(result, rowNumber, 7);
	int length = strlcpy(extConfig->nspname, value, NAMEDATALEN);

	if (length >= NAMEDATALEN)
	{
		log_error("Schema name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (NAMEDATALEN - 1)",
				  value, length, NAMEDATALEN - 1);
		++errors;
	}

	/* 9. c.relname */
	value = PQgetvalue(result, rowNumber, 8);
	length = strlcpy(extConfig->relname, value, NAMEDATALEN);

	if (length >= NAMEDATALEN)
	{
		log_error("Extension configuration table name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (NAMEDATALEN - 1)",
				  value, length, NAMEDATALEN - 1);
		++errors;
	}

	/* 10. extcondition */
	value = PQgetvalue(result, rowNumber, 9);
	extConfig->condition = strdup(value);

	if (extConfig->condition == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		++errors;
	}

	return errors == 0;
}


/*
 * getTableArray loops over the SQL result for the tables array query and
 * allocates an array of tables then populates it with the query result.
 */
static void
getTableArray(void *ctx, PGresult *result)
{
	SourceTableArrayContext *context = (SourceTableArrayContext *) ctx;
	int nTuples = PQntuples(result);

	log_debug("getTableArray: %d", nTuples);

	if (PQnfields(result) != 9)
	{
		log_error("Query returned %d columns, expected 9", PQnfields(result));
		context->parsedOk = false;
		return;
	}

	/* we're not supposed to re-cycle arrays here */
	if (context->tableArray->array != NULL)
	{
		/* issue a warning but let's try anyway */
		log_warn("BUG? context's array is not null in getTableArray");

		free(context->tableArray->array);
		context->tableArray->array = NULL;
	}

	context->tableArray->count = nTuples;
	context->tableArray->array =
		(SourceTable *) calloc(nTuples, sizeof(SourceTable));

	if (context->tableArray->array == NULL)
	{
		log_fatal(ALLOCATION_FAILED_ERROR);
		return;
	}

	bool parsedOk = true;

	for (int rowNumber = 0; rowNumber < nTuples; rowNumber++)
	{
		SourceTable *table = &(context->tableArray->array[rowNumber]);

		parsedOk = parsedOk &&
				   parseCurrentSourceTable(result, rowNumber, table);
	}

	if (!parsedOk)
	{
		free(context->tableArray->array);
		context->tableArray->array = NULL;
	}

	context->parsedOk = parsedOk;
}


/*
 * parseCurrentSourceTable parses a single row of the table listing query
 * result.
 */
static bool
parseCurrentSourceTable(PGresult *result, int rowNumber, SourceTable *table)
{
	int errors = 0;

	/* 1. c.oid */
	char *value = PQgetvalue(result, rowNumber, 0);

	if (!stringToUInt32(value, &(table->oid)) || table->oid == 0)
	{
		log_error("Invalid OID \"%s\"", value);
		++errors;
	}

	/* 2. n.nspname */
	value = PQgetvalue(result, rowNumber, 1);
	int length = strlcpy(table->nspname, value, NAMEDATALEN);

	if (length >= NAMEDATALEN)
	{
		log_error("Schema name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (NAMEDATALEN - 1)",
				  value, length, NAMEDATALEN - 1);
		++errors;
	}

	/* 3. c.relname */
	value = PQgetvalue(result, rowNumber, 2);
	length = strlcpy(table->relname, value, NAMEDATALEN);

	if (length >= NAMEDATALEN)
	{
		log_error("Table name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (NAMEDATALEN - 1)",
				  value, length, NAMEDATALEN - 1);
		++errors;
	}

	/* 4. c.reltuples::bigint */
	if (PQgetisnull(result, rowNumber, 3))
	{
		/*
		 * reltuples is NULL when table has never been ANALYZEd, just count
		 * zero then.
		 */
		table->reltuples = 0;
	}
	else
	{
		value = PQgetvalue(result, rowNumber, 3);

		if (!stringToInt64(value, &(table->reltuples)))
		{
			log_error("Invalid reltuples::bigint \"%s\"", value);
			++errors;
		}
	}

	/* 5. pg_table_size(c.oid) as bytes */
	value = PQgetvalue(result, rowNumber, 4);

	if (!stringToInt64(value, &(table->bytes)))
	{
		log_error("Invalid reltuples::bigint \"%s\"", value);
		++errors;
	}

	/* 6. pg_size_pretty(c.oid) */
	value = PQgetvalue(result, rowNumber, 5);
	length = strlcpy(table->bytesPretty, value, NAMEDATALEN);

	if (length >= NAMEDATALEN)
	{
		log_error("Pretty printed byte size \"%s\" is %d bytes long, "
				  "the maximum expected is %d (NAMEDATALEN - 1)",
				  value, length, NAMEDATALEN - 1);
		++errors;
	}

	/* 7. excludeData */
	value = PQgetvalue(result, rowNumber, 6);
	table->excludeData = (*value) == 't';

	/* 8. restoreListName */
	value = PQgetvalue(result, rowNumber, 7);
	length = strlcpy(table->restoreListName, value, RESTORE_LIST_NAMEDATALEN);

	if (length >= RESTORE_LIST_NAMEDATALEN)
	{
		log_error("Table restore list name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (RESTORE_LIST_NAMEDATALEN - 1)",
				  value, length, RESTORE_LIST_NAMEDATALEN - 1);
		++errors;
	}

	/* 9. partkey */
	if (PQgetisnull(result, rowNumber, 8))
	{
		log_debug("Table \"%s\".\"%s\" with oid %u has not part key column",
				  table->nspname,
				  table->relname,
				  table->oid);
	}
	else
	{
		value = PQgetvalue(result, rowNumber, 8);
		length = strlcpy(table->partKey, value, NAMEDATALEN);

		if (length >= NAMEDATALEN)
		{
			log_error("Partition key column name \"%s\" is %d bytes long, "
					  "the maximum expected is %d (NAMEDATALEN - 1)",
					  value, length, NAMEDATALEN - 1);
			++errors;
		}
	}

	log_trace("parseCurrentSourceTable: %s.%s", table->nspname, table->relname);

	return errors == 0;
}


/*
 * getSequenceArray loops over the SQL result for the sequence array query and
 * allocates an array of tables then populates it with the query result.
 */
static void
getSequenceArray(void *ctx, PGresult *result)
{
	SourceSequenceArrayContext *context = (SourceSequenceArrayContext *) ctx;
	int nTuples = PQntuples(result);

	log_debug("getSequenceArray: %d", nTuples);

	if (PQnfields(result) != 4)
	{
		log_error("Query returned %d columns, expected 4", PQnfields(result));
		context->parsedOk = false;
		return;
	}

	/* we're not supposed to re-cycle arrays here */
	if (context->sequenceArray->array != NULL)
	{
		/* issue a warning but let's try anyway */
		log_warn("BUG? context's array is not null in getSequenceArray");

		free(context->sequenceArray->array);
		context->sequenceArray->array = NULL;
	}

	context->sequenceArray->count = nTuples;
	context->sequenceArray->array =
		(SourceSequence *) calloc(nTuples, sizeof(SourceSequence));

	if (context->sequenceArray->array == NULL)
	{
		log_fatal(ALLOCATION_FAILED_ERROR);
		return;
	}

	bool parsedOk = true;

	for (int rowNumber = 0; rowNumber < nTuples; rowNumber++)
	{
		SourceSequence *sequence = &(context->sequenceArray->array[rowNumber]);

		parsedOk = parsedOk &&
				   parseCurrentSourceSequence(result, rowNumber, sequence);
	}

	if (!parsedOk)
	{
		free(context->sequenceArray->array);
		context->sequenceArray->array = NULL;
	}

	context->parsedOk = parsedOk;
}


/*
 * parseCurrentSourceSequence parses a single row of the table listing query
 * result.
 */
static bool
parseCurrentSourceSequence(PGresult *result, int rowNumber, SourceSequence *seq)
{
	int errors = 0;

	/* 1. c.oid */
	char *value = PQgetvalue(result, rowNumber, 0);

	if (!stringToUInt32(value, &(seq->oid)) || seq->oid == 0)
	{
		log_error("Invalid OID \"%s\"", value);
		++errors;
	}

	/* 2. n.nspname */
	value = PQgetvalue(result, rowNumber, 1);
	int length = strlcpy(seq->nspname, value, NAMEDATALEN);

	if (length >= NAMEDATALEN)
	{
		log_error("Schema name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (NAMEDATALEN - 1)",
				  value, length, NAMEDATALEN - 1);
		++errors;
	}

	/* 3. c.relname */
	value = PQgetvalue(result, rowNumber, 2);
	length = strlcpy(seq->relname, value, NAMEDATALEN);

	if (length >= NAMEDATALEN)
	{
		log_error("Sequence name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (NAMEDATALEN - 1)",
				  value, length, NAMEDATALEN - 1);
		++errors;
	}

	/* 7. indexRestoreListName */
	value = PQgetvalue(result, rowNumber, 3);
	length = strlcpy(seq->restoreListName, value, RESTORE_LIST_NAMEDATALEN);

	if (length >= RESTORE_LIST_NAMEDATALEN)
	{
		log_error("Table restore list name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (RESTORE_LIST_NAMEDATALEN - 1)",
				  value, length, RESTORE_LIST_NAMEDATALEN - 1);
		++errors;
	}

	return errors == 0;
}


/*
 * getTableArray loops over the SQL result for the tables array query and
 * allocates an array of tables then populates it with the query result.
 */
static void
getIndexArray(void *ctx, PGresult *result)
{
	SourceIndexArrayContext *context = (SourceIndexArrayContext *) ctx;
	int nTuples = PQntuples(result);

	log_debug("getIndexArray: %d", nTuples);

	if (PQnfields(result) != 14)
	{
		log_error("Query returned %d columns, expected 14", PQnfields(result));
		context->parsedOk = false;
		return;
	}

	/* we're not supposed to re-cycle arrays here */
	if (context->indexArray->array != NULL)
	{
		/* issue a warning but let's try anyway */
		log_warn("BUG? context's array is not null in getIndexArray");

		free(context->indexArray->array);
		context->indexArray->array = NULL;
	}

	context->indexArray->count = nTuples;
	context->indexArray->array =
		(SourceIndex *) calloc(nTuples, sizeof(SourceIndex));

	if (context->indexArray->array == NULL)
	{
		log_fatal(ALLOCATION_FAILED_ERROR);
		return;
	}

	bool parsedOk = true;

	for (int rowNumber = 0; rowNumber < nTuples; rowNumber++)
	{
		SourceIndex *index = &(context->indexArray->array[rowNumber]);

		parsedOk = parsedOk &&
				   parseCurrentSourceIndex(result, rowNumber, index);
	}

	if (!parsedOk)
	{
		free(context->indexArray->array);
		context->indexArray->array = NULL;
	}

	context->parsedOk = parsedOk;
}


/*
 * getTableArray loops over the SQL result for the tables array query and
 * allocates an array of tables then populates it with the query result.
 */
static bool
parseCurrentSourceIndex(PGresult *result, int rowNumber, SourceIndex *index)
{
	int errors = 0;

	/* 1. i.oid */
	char *value = PQgetvalue(result, rowNumber, 0);

	if (!stringToUInt32(value, &(index->indexOid)) || index->indexOid == 0)
	{
		log_error("Invalid index OID \"%s\"", value);
		++errors;
	}

	/* 2. n.nspname */
	value = PQgetvalue(result, rowNumber, 1);
	int length = strlcpy(index->indexNamespace, value, NAMEDATALEN);

	if (length >= NAMEDATALEN)
	{
		log_error("Schema name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (NAMEDATALEN - 1)",
				  value, length, NAMEDATALEN - 1);
		++errors;
	}

	/* 3. i.relname */
	value = PQgetvalue(result, rowNumber, 2);
	length = strlcpy(index->indexRelname, value, NAMEDATALEN);

	if (length >= NAMEDATALEN)
	{
		log_error("Index name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (NAMEDATALEN - 1)",
				  value, length, NAMEDATALEN - 1);
		++errors;
	}

	/* 4. r.oid */
	value = PQgetvalue(result, rowNumber, 3);

	if (!stringToUInt32(value, &(index->tableOid)) || index->tableOid == 0)
	{
		log_error("Invalid OID \"%s\"", value);
		++errors;
	}

	/* 5. rn.nspname */
	value = PQgetvalue(result, rowNumber, 4);
	length = strlcpy(index->tableNamespace, value, NAMEDATALEN);

	if (length >= NAMEDATALEN)
	{
		log_error("Schema name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (NAMEDATALEN - 1)",
				  value, length, NAMEDATALEN - 1);
		++errors;
	}

	/* 6. r.relname */
	value = PQgetvalue(result, rowNumber, 5);
	length = strlcpy(index->tableRelname, value, NAMEDATALEN);

	if (length >= NAMEDATALEN)
	{
		log_error("Index name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (NAMEDATALEN - 1)",
				  value, length, NAMEDATALEN - 1);
		++errors;
	}

	/* 7. indisprimary */
	value = PQgetvalue(result, rowNumber, 6);
	if (value == NULL || ((*value != 't') && (*value != 'f')))
	{
		log_error("Invalid indisprimary value \"%s\"", value);
		++errors;
	}
	else
	{
		index->isPrimary = (*value) == 't';
	}

	/* 8. indisunique */
	value = PQgetvalue(result, rowNumber, 7);
	if (value == NULL || ((*value != 't') && (*value != 'f')))
	{
		log_error("Invalid indisunique value \"%s\"", value);
		++errors;
	}
	else
	{
		index->isUnique = (*value) == 't';
	}

	/* 9. cols */
	value = PQgetvalue(result, rowNumber, 8);
	length = strlcpy(index->indexColumns, value, BUFSIZE);

	if (length >= BUFSIZE)
	{
		log_error("Index name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (BUFSIZE - 1)",
				  value, length, BUFSIZE - 1);
		++errors;
	}

	/* 10. pg_get_indexdef() */
	value = PQgetvalue(result, rowNumber, 9);
	length = strlcpy(index->indexDef, value, BUFSIZE);

	if (length >= BUFSIZE)
	{
		log_error("Index name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (BUFSIZE - 1)",
				  value, length, BUFSIZE - 1);
		++errors;
	}

	/* 11. c.oid */
	if (PQgetisnull(result, rowNumber, 10))
	{
		index->constraintOid = 0;
	}
	else
	{
		value = PQgetvalue(result, rowNumber, 10);

		if (!stringToUInt32(value, &(index->constraintOid)) ||
			index->constraintOid == 0)
		{
			log_error("Invalid OID \"%s\"", value);
			++errors;
		}
	}

	/* 12. conname */
	if (!PQgetisnull(result, rowNumber, 11))
	{
		value = PQgetvalue(result, rowNumber, 11);
		length = strlcpy(index->constraintName, value, NAMEDATALEN);

		if (length >= NAMEDATALEN)
		{
			log_error("Index name \"%s\" is %d bytes long, "
					  "the maximum expected is %d (NAMEDATALEN - 1)",
					  value, length, NAMEDATALEN - 1);
			++errors;
		}
	}

	/* 13. pg_get_constraintdef */
	if (!PQgetisnull(result, rowNumber, 12))
	{
		value = PQgetvalue(result, rowNumber, 12);
		length = strlcpy(index->constraintDef, value, BUFSIZE);

		if (length >= BUFSIZE)
		{
			log_error("Index name \"%s\" is %d bytes long, "
					  "the maximum expected is %d (BUFSIZE - 1)",
					  value, length, BUFSIZE - 1);
			++errors;
		}
	}

	/* 14. indexRestoreListName */
	value = PQgetvalue(result, rowNumber, 13);
	length =
		strlcpy(index->indexRestoreListName, value, RESTORE_LIST_NAMEDATALEN);

	if (length >= RESTORE_LIST_NAMEDATALEN)
	{
		log_error("Index restore list name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (RESTORE_LIST_NAMEDATALEN - 1)",
				  value, length, RESTORE_LIST_NAMEDATALEN - 1);
		++errors;
	}

	return errors == 0;
}


/*
 * getDependArray loops over the SQL result for the table dependencies array
 * query and allocates an array of tables then populates it with the query
 * result.
 */
static void
getDependArray(void *ctx, PGresult *result)
{
	SourceDependArrayContext *context = (SourceDependArrayContext *) ctx;
	int nTuples = PQntuples(result);

	log_debug("getDependArray: %d", nTuples);

	if (PQnfields(result) != 9)
	{
		log_error("Query returned %d columns, expected 9", PQnfields(result));
		context->parsedOk = false;
		return;
	}

	/* we're not supposed to re-cycle arrays here */
	if (context->dependArray->array != NULL)
	{
		/* issue a warning but let's try anyway */
		log_warn("BUG? context's array is not null in getDependArray");

		free(context->dependArray->array);
		context->dependArray->array = NULL;
	}

	context->dependArray->count = nTuples;
	context->dependArray->array =
		(SourceDepend *) calloc(nTuples, sizeof(SourceDepend));

	if (context->dependArray->array == NULL)
	{
		log_fatal(ALLOCATION_FAILED_ERROR);
		return;
	}

	bool parsedOk = true;

	for (int rowNumber = 0; rowNumber < nTuples; rowNumber++)
	{
		SourceDepend *depend = &(context->dependArray->array[rowNumber]);

		parsedOk = parsedOk &&
				   parseCurrentSourceDepend(result, rowNumber, depend);
	}

	if (!parsedOk)
	{
		free(context->dependArray->array);
		context->dependArray->array = NULL;
	}

	context->parsedOk = parsedOk;
}


/*
 * parseCurrentSourceDepend parses a single row of the table listing query
 * result.
 */
static bool
parseCurrentSourceDepend(PGresult *result, int rowNumber, SourceDepend *depend)
{
	int errors = 0;

	/* 1. n.nspname */
	char *value = PQgetvalue(result, rowNumber, 0);
	int length = strlcpy(depend->nspname, value, NAMEDATALEN);

	if (length >= NAMEDATALEN)
	{
		log_error("Schema name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (NAMEDATALEN - 1)",
				  value, length, NAMEDATALEN - 1);
		++errors;
	}

	/* 2. c.relname */
	value = PQgetvalue(result, rowNumber, 1);
	length = strlcpy(depend->relname, value, NAMEDATALEN);

	if (length >= NAMEDATALEN)
	{
		log_error("Table name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (NAMEDATALEN - 1)",
				  value, length, NAMEDATALEN - 1);
		++errors;
	}

	/* 3. refclassid */
	if (PQgetisnull(result, rowNumber, 2))
	{
		depend->refclassid = 0;
	}
	else
	{
		value = PQgetvalue(result, rowNumber, 2);

		if (!stringToUInt32(value, &(depend->refclassid)) || depend->refclassid == 0)
		{
			log_error("Invalid OID \"%s\"", value);
			++errors;
		}
	}

	/* 4. refobjid */
	if (PQgetisnull(result, rowNumber, 3))
	{
		depend->refobjid = 0;
	}
	else
	{
		value = PQgetvalue(result, rowNumber, 3);

		if (!stringToUInt32(value, &(depend->refobjid)) || depend->refobjid == 0)
		{
			log_error("Invalid OID \"%s\"", value);
			++errors;
		}
	}

	/* 5. classid */
	if (PQgetisnull(result, rowNumber, 4))
	{
		depend->classid = 0;
	}
	else
	{
		value = PQgetvalue(result, rowNumber, 4);

		if (!stringToUInt32(value, &(depend->classid)) || depend->classid == 0)
		{
			log_error("Invalid OID \"%s\"", value);
			++errors;
		}
	}

	/* 6. objid */
	if (PQgetisnull(result, rowNumber, 5))
	{
		depend->objid = 0;
	}
	else
	{
		value = PQgetvalue(result, rowNumber, 5);

		if (!stringToUInt32(value, &(depend->objid)) || depend->objid == 0)
		{
			log_error("Invalid OID \"%s\"", value);
			++errors;
		}
	}

	/* 7. deptype */
	if (PQgetisnull(result, rowNumber, 6))
	{
		depend->deptype = 's';  /* invent something for schemas */
	}
	else
	{
		value = PQgetvalue(result, rowNumber, 6);
		depend->deptype = value[0];
	}

	/* 8. type */
	value = PQgetvalue(result, rowNumber, 7);
	length = strlcpy(depend->type, value, BUFSIZE);

	if (length >= BUFSIZE)
	{
		log_error("Table dependency type \"%s\" is %d bytes long, "
				  "the maximum expected is %d (BUFSIZE - 1)",
				  value, length, BUFSIZE - 1);
		++errors;
	}

	/* 9. identity */
	value = PQgetvalue(result, rowNumber, 8);
	length = strlcpy(depend->identity, value, BUFSIZE);

	if (length >= BUFSIZE)
	{
		log_error("Table dependency identity \"%s\" is %d bytes long, "
				  "the maximum expected is %d (BUFSIZE - 1)",
				  value, length, BUFSIZE - 1);
		++errors;
	}

	return errors == 0;
}


/*
 * getPartitionList loops over the SQL result for the COPY partitions query and
 * allocate an array of SourceTableParts and populates it with the query
 * results.
 */
static void
getPartitionList(void *ctx, PGresult *result)
{
	SourcePartitionContext *context = (SourcePartitionContext *) ctx;
	int nTuples = PQntuples(result);

	if (PQnfields(result) != 5)
	{
		log_error("Query returned %d columns, expected 5", PQnfields(result));
		context->parsedOk = false;
		return;
	}

	/* we're not supposed to re-cycle arrays here */
	if (context->table->partsArray.array != NULL)
	{
		/* issue a warning but let's try anyway */
		log_warn("BUG? context's partsArray is not null in getPartitionList");

		free(context->table->partsArray.array);
		context->table->partsArray.array = NULL;
		context->table->partsArray.count = 0;
	}

	context->table->partsArray.count = nTuples;
	context->table->partsArray.array =
		(SourceTableParts *) calloc(nTuples, sizeof(SourceTableParts));

	if (context->table->partsArray.array == NULL)
	{
		log_fatal(ALLOCATION_FAILED_ERROR);
		return;
	}

	bool parsedOk = true;

	for (int rowNumber = 0; rowNumber < nTuples; rowNumber++)
	{
		SourceTableParts *parts = &(context->table->partsArray.array[rowNumber]);

		parsedOk = parsedOk &&
				   parseCurrentPartition(result, rowNumber, parts);
	}

	if (!parsedOk)
	{
		free(context->table->partsArray.array);
		context->table->partsArray.array = NULL;
		context->table->partsArray.count = 0;
	}

	context->parsedOk = parsedOk;
}


/*
 * parseCurrentPartition parses a single row of the table COPY partition
 * listing query result.
 */
static bool
parseCurrentPartition(PGresult *result, int rowNumber, SourceTableParts *parts)
{
	int errors = 0;

	/* 1. partNumber */
	char *value = PQgetvalue(result, rowNumber, 0);

	if (!stringToInt(value, &(parts->partNumber)))
	{
		log_error("Invalid part number \"%s\"", value);
		++errors;
	}

	/* 2. partCount */
	value = PQgetvalue(result, rowNumber, 1);

	if (!stringToInt(value, &(parts->partCount)))
	{
		log_error("Invalid part count \"%s\"", value);
		++errors;
	}

	/* 3. min */
	value = PQgetvalue(result, rowNumber, 2);

	if (!stringToInt64(value, &(parts->min)))
	{
		log_error("Invalid part min \"%s\"", value);
		++errors;
	}

	/* 4. max */
	value = PQgetvalue(result, rowNumber, 3);

	if (!stringToInt64(value, &(parts->max)))
	{
		log_error("Invalid part max \"%s\"", value);
		++errors;
	}

	/* 5. count */
	value = PQgetvalue(result, rowNumber, 4);

	if (!stringToInt64(value, &(parts->count)))
	{
		log_error("Invalid part count \"%s\"", value);
		++errors;
	}

	return errors == 0;
}
