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
#include "filtering.h"
#include "log.h"
#include "parsing.h"
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

struct FilteringQueries
{
	SourceFilterType type;
	char *sql;
};

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
		"                regexp_replace(auth.rolname, '[\n\r]', ' '))"
		"    from pg_catalog.pg_class c "
		"         join pg_catalog.pg_namespace n on c.relnamespace = n.oid "
		"         join pg_authid auth ON auth.oid = c.relowner"
		"   where c.relkind = 'r' and c.relpersistence = 'p' "
		"     and n.nspname !~ '^pg_' and n.nspname <> 'information_schema' "
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
		"                regexp_replace(auth.rolname, '[\n\r]', ' '))"
		"    from pg_catalog.pg_class c "
		"         join pg_catalog.pg_namespace n on c.relnamespace = n.oid "
		"         join pg_authid auth ON auth.oid = c.relowner"

		/* include-only-table */
		"         join pg_temp.filter_include_only_table inc "
		"           on n.nspname = inc.nspname "
		"          and c.relname = inc.relname "

		"   where c.relkind = 'r' and c.relpersistence = 'p' "
		"     and n.nspname !~ '^pg_' and n.nspname <> 'information_schema' "
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
		"                regexp_replace(auth.rolname, '[\n\r]', ' '))"
		"    from pg_catalog.pg_class c "
		"         join pg_catalog.pg_namespace n on c.relnamespace = n.oid "
		"         join pg_authid auth ON auth.oid = c.relowner"

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

		"   where c.relkind = 'r' and c.relpersistence = 'p' "
		"     and n.nspname !~ '^pg_' and n.nspname <> 'information_schema' "

		/* WHERE clause for exclusion filters */
		"     and fn.nspname is null "
		"     and ft.relname is null "
		"     and ftd.relname is null "

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
		"                regexp_replace(auth.rolname, '[\n\r]', ' '))"
		"    from pg_catalog.pg_class c "
		"         join pg_catalog.pg_namespace n on c.relnamespace = n.oid "
		"         join pg_authid auth ON auth.oid = c.relowner"

		/* include-only-table */
		"    left join pg_temp.filter_include_only_table inc "
		"           on n.nspname = inc.nspname "
		"          and c.relname = inc.relname "

		"   where c.relkind = 'r' and c.relpersistence = 'p' "
		"     and n.nspname !~ '^pg_' and n.nspname <> 'information_schema' "

		/* WHERE clause for exclusion filters */
		"     and inc.nspname is null "

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
		"                regexp_replace(auth.rolname, '[\n\r]', ' '))"
		"    from pg_catalog.pg_class c "
		"         join pg_catalog.pg_namespace n on c.relnamespace = n.oid "
		"         join pg_authid auth ON auth.oid = c.relowner"

		/* exclude-schema */
		"         left join pg_temp.filter_exclude_schema fn "
		"                on n.nspname = fn.nspname "

		/* exclude-table */
		"         left join pg_temp.filter_exclude_table ft "
		"                on n.nspname = ft.nspname "
		"               and c.relname = ft.relname "

		/* WHERE clause for exclusion filters */
		"     and (   fn.nspname is not null "
		"          or ft.relname is not null ) "

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
		"         format('%s %s %s', "
		"                regexp_replace(n.nspname, '[\n\r]', ' '), "
		"                regexp_replace(r.relname, '[\n\r]', ' '), "
		"                regexp_replace(auth.rolname, '[\n\r]', ' '))"
		"    from pg_class r "
		"         join pg_namespace n ON n.oid = r.relnamespace "
		"         join pg_authid auth ON auth.oid = r.relowner"
		"   where r.relkind = 'r' and r.relpersistence = 'p'  "
		"     and n.nspname !~ '^pg_' and n.nspname <> 'information_schema' "
		"     and not exists "
		"         ( "
		"           select c.oid "
		"             from pg_constraint c "
		"            where c.conrelid = r.oid "
		"              and c.contype = 'p' "
		"         ) "
		"order by n.nspname, r.relname"
	},

	{
		SOURCE_FILTER_TYPE_INCL,

		"  select r.oid, n.nspname, r.relname, r.reltuples::bigint, "
		"         pg_table_size(r.oid) as bytes, "
		"         pg_size_pretty(pg_table_size(r.oid)), "
		"         format('%s %s %s', "
		"                regexp_replace(n.nspname, '[\n\r]', ' '), "
		"                regexp_replace(r.relname, '[\n\r]', ' '), "
		"                regexp_replace(auth.rolname, '[\n\r]', ' '))"
		"    from pg_class r "
		"         join pg_namespace n ON n.oid = r.relnamespace "
		"         join pg_authid auth ON auth.oid = r.relowner"

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
		"order by n.nspname, r.relname"
	},

	{
		SOURCE_FILTER_TYPE_EXCL,

		"  select r.oid, n.nspname, r.relname, r.reltuples::bigint, "
		"         pg_table_size(r.oid) as bytes, "
		"         pg_size_pretty(pg_table_size(r.oid)), "
		"         format('%s %s %s', "
		"                regexp_replace(n.nspname, '[\n\r]', ' '), "
		"                regexp_replace(r.relname, '[\n\r]', ' '), "
		"                regexp_replace(auth.rolname, '[\n\r]', ' '))"
		"    from pg_class r "
		"         join pg_namespace n ON n.oid = r.relnamespace "
		"         join pg_authid auth ON auth.oid = r.relowner"

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

		"order by n.nspname, r.relname"
	},

	{
		SOURCE_FILTER_TYPE_LIST_NOT_INCL,

		"  select r.oid, n.nspname, r.relname, r.reltuples::bigint, "
		"         pg_table_size(r.oid) as bytes, "
		"         pg_size_pretty(pg_table_size(r.oid)), "
		"         format('%s %s %s', "
		"                regexp_replace(n.nspname, '[\n\r]', ' '), "
		"                regexp_replace(r.relname, '[\n\r]', ' '), "
		"                regexp_replace(auth.rolname, '[\n\r]', ' '))"
		"    from pg_class r "
		"         join pg_namespace n ON n.oid = r.relnamespace "
		"         join pg_authid auth ON auth.oid = r.relowner"

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

		"order by n.nspname, r.relname"
	},

	{
		SOURCE_FILTER_TYPE_LIST_EXCL,

		"  select r.oid, n.nspname, r.relname, r.reltuples::bigint, "
		"         pg_table_size(r.oid) as bytes, "
		"         pg_size_pretty(pg_table_size(r.oid)), "
		"         format('%s %s %s', "
		"                regexp_replace(n.nspname, '[\n\r]', ' '), "
		"                regexp_replace(r.relname, '[\n\r]', ' '), "
		"                regexp_replace(auth.rolname, '[\n\r]', ' '))"
		"    from pg_class r "
		"         join pg_namespace n ON n.oid = r.relnamespace "
		"         join pg_authid auth ON auth.oid = r.relowner"

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
		"         join pg_authid auth ON auth.oid = c.relowner"
		"   where c.relkind = 'S' and c.relpersistence = 'p' "
		"     and n.nspname !~ '^pg_' and n.nspname <> 'information_schema' "
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
		"         join pg_authid auth ON auth.oid = s.relowner"
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
		"         join pg_authid auth ON auth.oid = s.relowner"
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
		"         join pg_authid auth ON auth.oid = s.relowner"
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
		"         join pg_authid auth ON auth.oid = s.relowner"
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
		"          join pg_authid auth ON auth.oid = i.relowner"
		"          left join pg_depend d "
		"                 on d.classid = 'pg_class'::regclass"
		"                and d.objid = i.oid"
		"                and d.refclassid = 'pg_constraint'::regclass"
		"                and d.deptype = 'i'"
		"          left join pg_constraint c ON c.oid = d.refobjid"
		"    where r.relkind = 'r' and r.relpersistence = 'p' "
		"      and n.nspname !~ '^pg_' and n.nspname <> 'information_schema'"
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
		"          join pg_authid auth ON auth.oid = i.relowner"
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
		"          join pg_authid auth ON auth.oid = i.relowner"
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
		"          join pg_authid auth ON auth.oid = i.relowner"
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
		"          join pg_authid auth ON auth.oid = i.relowner"
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
		"          join pg_authid auth ON auth.oid = i.relowner"
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
		"          join pg_authid auth ON auth.oid = i.relowner"
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
		"          join pg_authid auth ON auth.oid = i.relowner"
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
		"  SELECT n.nspname, c.relname, "
		"         refclassid, refobjid, classid, objid, "
		"         deptype, type, identity "
		"    FROM unconcat "

		"         join pg_class c "
		"           on unconcat.refclassid = 'pg_class'::regclass "
		"          and unconcat.refobjid = c.oid "

		"         join pg_catalog.pg_namespace n on c.relnamespace = n.oid "

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

		"         , pg_identify_object(classid, objid, objsubid) "

		"   WHERE NOT (refclassid = classid AND refobjid = objid) "
		"      and n.nspname !~ '^pg_' and n.nspname <> 'information_schema'"
		"      and type not in ('toast table column', 'default value') "

		/* WHERE clause for exclusion filters */
		"     and fn.nspname is null "
		"     and ft.relname is null "
		"     and ftd.relname is null "

		/* remove duplicates due to multiple refobjsubid / objsubid */
		"GROUP BY n.nspname, c.relname, "
		"         refclassid, refobjid, classid, objid, deptype, type, identity"
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
		"  SELECT n.nspname, c.relname, "
		"         refclassid, refobjid, classid, objid, "
		"         deptype, type, identity "
		"    FROM unconcat "

		"         join pg_class c "
		"           on unconcat.refclassid = 'pg_class'::regclass "
		"          and unconcat.refobjid = c.oid "

		"         join pg_catalog.pg_namespace n on c.relnamespace = n.oid "

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
 * getTableArray loops over the SQL result for the tables array query and
 * allocates an array of tables then populates it with the query result.
 */
static void
getTableArray(void *ctx, PGresult *result)
{
	SourceTableArrayContext *context = (SourceTableArrayContext *) ctx;
	int nTuples = PQntuples(result);

	log_trace("getTableArray: %d", nTuples);

	if (PQnfields(result) != 8)
	{
		log_error("Query returned %d columns, expected 8", PQnfields(result));
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
		(SourceTable *) malloc(nTuples * sizeof(SourceTable));

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
	value = PQgetvalue(result, rowNumber, 3);

	if (!stringToInt64(value, &(table->reltuples)))
	{
		log_error("Invalid reltuples::bigint \"%s\"", value);
		++errors;
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

	log_trace("getSequenceArray: %d", nTuples);

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
		(SourceSequence *) malloc(nTuples * sizeof(SourceSequence));

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

	log_trace("getIndexArray: %d", nTuples);

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
		(SourceIndex *) malloc(nTuples * sizeof(SourceIndex));

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

	log_trace("getDependArray: %d", nTuples);

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
		(SourceDepend *) malloc(nTuples * sizeof(SourceDepend));

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
	value = PQgetvalue(result, rowNumber, 2);

	if (!stringToUInt32(value, &(depend->refclassid)) || depend->refclassid == 0)
	{
		log_error("Invalid OID \"%s\"", value);
		++errors;
	}

	/* 4. refobjid */
	value = PQgetvalue(result, rowNumber, 3);

	if (!stringToUInt32(value, &(depend->refobjid)) || depend->refobjid == 0)
	{
		log_error("Invalid OID \"%s\"", value);
		++errors;
	}

	/* 5. classid */
	value = PQgetvalue(result, rowNumber, 4);

	if (!stringToUInt32(value, &(depend->classid)) || depend->classid == 0)
	{
		log_error("Invalid OID \"%s\"", value);
		++errors;
	}

	/* 6. objid */
	value = PQgetvalue(result, rowNumber, 5);

	if (!stringToUInt32(value, &(depend->objid)) || depend->objid == 0)
	{
		log_error("Invalid OID \"%s\"", value);
		++errors;
	}

	/* 7. deptype */
	value = PQgetvalue(result, rowNumber, 6);
	depend->deptype = value[0];

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
