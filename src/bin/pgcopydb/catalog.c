/*
 * src/bin/pgcopydb/catalog.c
 *	 Catalog management as a SQLite internal file
 */

#include <inttypes.h>
#include <limits.h>
#include <stdlib.h>
#include <sys/select.h>
#include <time.h>
#include <unistd.h>

#include "sqlite3.h"

#include "catalog.h"
#include "copydb.h"
#include "defaults.h"
#include "log.h"
#include "parsing_utils.h"
#include "schema.h"
#include "signals.h"
#include "string_utils.h"
#include "summary.h"


/*
 * pgcopydb catalog cache is a SQLite database with the following schema:
 */
static char *sourceDBcreateDDLs[] = {
	"create table setup("
	"  id integer primary key check (id = 1), "
	"  source_pg_uri text, "
	"  target_pg_uri text, "
	"  snapshot text, "
	"  split_tables_larger_than integer, "
	"  split_max_parts integer, "
	"  filters text, "
	"  plugin text, "
	"  slot_name text "
	")",

	"create table section("
	"  name text primary key, fetched boolean, "
	"  start_time_epoch integer, done_time_epoch integer, duration integer"
	")",

	"create table s_database("
	"  oid integer primary key, datname text, bytes integer, bytes_pretty text"
	")",

	"create table s_database_property("
	"  role_in_database boolean, rolname text, datname text, setconfig text"
	")",

	"create index s_d_p_oid on s_database_property(datname)",

	"create table s_namespace("
	"  nspname text primary key, restore_list_name text"
	")",

	"create index s_n_rlname on s_namespace(restore_list_name)",

	"create table s_table("
	"  oid integer primary key, "
	"  datname text, qname text, nspname text, relname text, amname text, "
	"  restore_list_name text, "
	"  relpages integer, reltuples integer, "
	"  exclude_data boolean, "
	"  part_key text"
	")",

	"create unique index s_t_qname on s_table(qname)",
	"create unique index s_t_rlname on s_table(restore_list_name)",

	"create table s_matview("
	"  oid integer primary key, "
	"  qname text, nspname text, relname text, restore_list_name text, "
	"  exclude_data boolean"
	")",

	"create unique index s_mv_rlname on s_matview(restore_list_name)",
	"create unique index s_mv_qname on s_matview(nspname, relname)",

	"create table s_table_size("
	"  oid integer primary key references s_table(oid), "
	"  bytes integer, bytes_pretty text "
	")",

	"create unique index s_ts_oid on s_table_size(oid)",

	"create table s_attr("
	"  oid integer references s_table(oid), "
	"  attnum integer, attypid integer, attname text, "
	"  attisprimary bool, attisgenerated bool, "
	"  primary key(oid, attnum) "
	")",

	"create index s_a_oid_attname on s_attr(oid, attname)",

	/* index for filtering out generated columns */
	"create index s_a_attisgenerated on s_attr(attisgenerated) where attisgenerated",

	"create table s_table_part("
	"  oid integer references s_table(oid), "
	"  partnum integer, partcount integer, "
	"  min integer, max integer, count integer, "
	"  primary key(oid, partnum) "
	")",

	"create table s_table_chksum("
	"  oid integer primary key references s_table(oid), "
	"  srcrowcount integer, srcsum text, dstrowcount integer, dstsum text "
	")",

	"create table s_index("
	"  oid integer primary key, "
	"  qname text, nspname text, relname text, restore_list_name text, "
	"  tableoid references s_table(oid), "
	"  isprimary bool, isunique bool, columns text, sql text "
	")",

	"create unique index s_i_rlname on s_index(restore_list_name)",

	"create table s_constraint("
	"  oid integer primary key, conname text, "
	"  indexoid references s_index(oid), "
	"  condeferrable bool, condeferred bool, sql text "
	")",

	"create table s_seq("
	"  oid integer, "
	"  ownedby integer, attrelid integer, attroid integer, "
	"  datname text, qname text, nspname text, relname text, "
	"  restore_list_name text, "
	"  last_value integer, isCalled bool, "
	"  primary key(oid, ownedby, attrelid, attroid)"
	")",

	"create index s_s_rlname on s_seq(restore_list_name)",

	/* internal activity tracking / completion / statistics */
	"create table process("
	"  pid integer primary key, "
	"  ps_type text, ps_title text, "
	"  tableoid integer references s_table(oid), "
	"  partnum integer, "
	"  indexoid integer references s_index(oid) "
	")",

	"create table timings("
	"  id integer primary key,"
	"  label text,"
	"  start_time_epoch integer, done_time_epoch integer, duration integer, "
	"  duration_pretty, "
	"  count integer, bytes integer, bytes_pretty text"
	")",

	"create table summary("
	"  pid integer, "
	"  tableoid integer references s_table(oid), "
	"  partnum integer, "
	"  indexoid integer references s_index(oid), "
	"  conoid integer references s_constraint(oid), "
	"  start_time_epoch integer, done_time_epoch integer, duration integer, "
	"  bytes integer, "
	"  command text, "
	"  unique(tableoid, partnum)"
	")",

	"create table vacuum_summary("
	"  pid integer, "
	"  tableoid integer references s_table(oid), "
	"  start_time_epoch integer, done_time_epoch integer, duration integer, "
	"  unique(tableoid)"
	")",

	"create table s_table_parts_done("
	" tableoid integer primary key references s_table(oid), pid integer"
	")",

	"create table s_table_indexes_done("
	" tableoid integer primary key references s_table(oid), pid integer "
	")",

	/* use SQLite more general dynamic type system: pg_lsn is text */
	"create table sentinel("
	"  id integer primary key check (id = 1), "
	"  startpos pg_lsn, endpos pg_lsn, apply bool, "
	" write_lsn pg_lsn, flush_lsn pg_lsn, replay_lsn pg_lsn)",

	"create table timeline_history("
	"  tli integer primary key, startpos pg_lsn, endpos pg_lsn)"
};


/*
 * pgcopydb implements filtering which needs to be implement by editin the
 * `pg_restore --list` archive TOC. The TOC contains OIDs "restore list names",
 * and some TOC entries do not have an OID.
 *
 * pgcopydb catalog cache needs to enable matching TOC entries by either OID
 * for restore list names for the main SQL objects (tables, indexes,
 * constraints, dependencies).
 *
 * The schema definition used for those objects is the same as in the previous
 * section, but the data is different and the points in the code where the
 * filters are used are limited in scope, in such a way that it makes sense to
 * maintain a separate SQLite database for the filters catalog cache.
 *
 */
static char *filterDBcreateDDLs[] = {
	"create table section("
	"  name text primary key, fetched boolean, "
	"  start_time_epoch integer, done_time_epoch integer, duration integer"
	")",

	"create table s_coll("
	"  oid integer primary key, collname text, description text, "
	"  restore_list_name text"
	")",

	"create unique index s_coll_rlname on s_coll(restore_list_name)",

	"create table s_extension("
	"  oid integer primary key, extname text, extnamespace text, "
	"  extrelocatable integer "
	")",

	"create table s_extension_config("
	"  extoid integer references s_extension(oid), "
	"  reloid integer, nspname text, relname text, condition text, "
	"  relkind integer "
	")",

	"create index s_ec_oid on s_extension_config(extoid)",

	"create table s_extension_versions("
	"  oid integer, name text, default_version text, installed_version text, "
	"  versions_array text, "
	"  primary key (oid, name)"
	")",

	"create table s_namespace("
	"  oid integer primary key, nspname text, restore_list_name text "
	")",

	"create index s_n_rlname on s_namespace(restore_list_name)",

	"create table s_table("
	"  oid integer primary key, "
	"  datname text, qname text, nspname text, relname text, amname text, "
	"  restore_list_name text, "
	"  relpages integer, reltuples integer, "
	"  exclude_data boolean, "
	"  srcrowcount integer, srcsum text, dstrowcount integer, dstsum text, "
	"  part_key text"
	")",

	"create unique index s_t_qname on s_table(qname)",
	"create unique index s_t_rlname on s_table(restore_list_name)",

	"create table s_matview("
	"  oid integer primary key, "
	"  qname text, nspname text, relname text, restore_list_name text, "
	"  exclude_data boolean"
	")",

	"create unique index s_mv_rlname on s_matview(restore_list_name)",
	"create unique index s_mv_qname on s_matview(nspname, relname)",

	"create table s_table_size("
	"  oid integer primary key references s_table(oid), "
	"  bytes integer, bytes_pretty text "
	")",

	"create unique index s_ts_oid on s_table_size(oid)",

	"create table s_attr("
	"  oid integer references s_table(oid), "
	"  attnum integer, attypid integer, attname text, "
	"  attisprimary bool, attisgenerated bool, "
	"  primary key(oid, attnum) "
	")",

	"create table s_table_part("
	"  oid integer references s_table(oid), "
	"  partnum integer, partcount integer, "
	"  min integer, max integer, count integer, "
	"  primary key(oid, partnum) "
	")",

	"create table s_table_chksum("
	"  oid integer primary key references s_table(oid), "
	"  srcrowcount integer, srcsum text, dstrowcount integer, dstsum text "
	")",

	"create table s_index("
	"  oid integer primary key, "
	"  qname text, nspname text, relname text, restore_list_name text, "
	"  tableoid references s_table(oid), "
	"  isprimary bool, isunique bool, columns text, sql text "
	")",

	"create unique index s_i_rlname on s_index(restore_list_name)",

	"create table s_constraint("
	"  oid integer primary key, conname text, "
	"  indexoid references s_index(oid), "
	"  condeferrable bool, condeferred bool, sql text "
	")",

	"create table s_seq("
	"  oid integer, "
	"  ownedby integer, attrelid integer, attroid integer, "
	"  datname text, qname text, nspname text, relname text, "
	"  restore_list_name text, "
	"  last_value integer, isCalled bool, "
	"  primary key(oid, ownedby, attrelid, attroid)"
	")",

	"create index s_s_rlname on s_seq(restore_list_name)",

	"create table s_depend("
	"  nspname text, relname text, "
	"  refclassid integer, refobjid integer, classid integer, objid integer, "
	"  deptype text, type text, identity text "
	")",

	"create index s_d_refobjid on s_depend(refobjid)",
	"create index s_d_objid on s_depend(objid)",

	/* the filter table is our hash-table */
	"create table filter(oid integer, restore_list_name text, kind text)",
	"create unique index filter_oid on filter(oid) where oid > 0",

	"create unique index filter_oid_rlname on filter(oid, restore_list_name) "
	" where oid > 0",

	"create index filter_rlname on filter(restore_list_name)",

	/*
	 * While we don't use a summary table in the filter database, some queries
	 * that are meant to work on both filters database and source database use
	 * LEFT JOIN summary.
	 */
	"create table summary("
	"  pid integer, "
	"  tableoid integer references s_table(oid), "
	"  partnum integer, "
	"  indexoid integer references s_index(oid), "
	"  conoid integer references s_constraint(oid), "
	"  start_time_epoch integer, done_time_epoch integer, duration integer, "
	"  bytes integer, "
	"  command text, "
	"  unique(tableoid, partnum)"
	")",
};


/*
 * Target schema objects, allowing to skip pre-existing entries.
 */
static char *targetDBcreateDDLs[] = {
	"create table section("
	"  name text primary key, fetched boolean, "
	"  start_time_epoch integer, done_time_epoch integer, duration integer"
	")",

	"create table s_role("
	"  oid integer primary key, rolname text"
	")",

	"create table s_namespace("
	"  nspname text primary key, restore_list_name text"
	")",

	"create index s_n_rlname on s_namespace(restore_list_name)",

	"create table s_table("
	"  oid integer primary key, "
	"  datname text, qname text, nspname text, relname text, amname text, "
	"  restore_list_name text, "
	"  relpages integer, reltuples integer, "
	"  exclude_data boolean, "
	"  srcrowcount integer, srcsum text, dstrowcount integer, dstsum text, "
	"  part_key text"
	")",

	"create unique index s_t_qname on s_table(qname)",
	"create unique index s_t_rlname on s_table(restore_list_name)",

	"create table s_attr("
	"  oid integer references s_table(oid), "
	"  attnum integer, attypid integer, attname text, "
	"  attisprimary bool, attisgenerated bool, "
	"  primary key(oid, attnum) "
	")",

	"create table s_index("
	"  oid integer primary key, "
	"  qname text, nspname text, relname text, restore_list_name text, "
	"  tableoid integer references s_table(oid), "
	"  isprimary bool, isunique bool, columns text, sql text "
	")",

	"create unique index s_i_rlname on s_index(restore_list_name)",

	"create table s_constraint("
	"  oid integer primary key, conname text, "
	"  indexoid references s_index(oid), "
	"  condeferrable bool, condeferred bool, sql text "
	")"
};


static char *sourceDBdropDDLs[] = {
	"drop table if exists setup",
	"drop table if exists section",

	"drop table if exists s_database",
	"drop table if exists s_database_property",
	"drop table if exists s_table",
	"drop table if exists s_matview",
	"drop table if exists s_attr",
	"drop table if exists s_table_part",
	"drop table if exists s_table_chksum",
	"drop table if exists s_table_size",
	"drop table if exists s_index",
	"drop table if exists s_constraint",
	"drop table if exists s_seq",
	"drop table if exists s_depend",

	"drop table if exists t_roles",
	"drop table if exists t_schema",
	"drop table if exists t_index",
	"drop table if exists t_constraint",

	"drop table if exists process",
	"drop table if exists summary",
	"drop table if exists s_table_parts_done",
	"drop table if exists s_table_indexes_done",

	"drop table if exists sentinel",
	"drop table if exists timeline_history"
};


static char *filterDBdropDDLs[] = {
	"drop table if exists section",

	"drop table if exists s_coll",
	"drop table if exists s_extension",
	"drop table if exists s_extension_config",
	"drop table if exists s_extension_versions",
	"drop table if exists s_namespace",
	"drop table if exists s_table",
	"drop table if exists s_matview",
	"drop table if exists s_attr",
	"drop table if exists s_table_part",
	"drop table if exists s_table_chksum",
	"drop table if exists s_table_size",
	"drop table if exists s_index",
	"drop table if exists s_constraint",
	"drop table if exists s_seq",
	"drop table if exists s_depend",
	"drop table if exists filter",
	"drop table if exists summary"
};


static char *targetDBdropDDLs[] = {
	"drop table if exists section",

	"drop table if exists s_role",
	"drop table if exists s_namespace",
	"drop table if exists s_table",
	"drop table if exists s_attr",
	"drop table if exists s_index",
	"drop table if exists s_constraint"
};


/*
 * catalog_init_from_specs initializes our internal catalog database file from
 * a specification.
 */
bool
catalog_init_from_specs(CopyDataSpec *copySpecs)
{
	DatabaseCatalog *sourceDB = &(copySpecs->catalogs.source);
	DatabaseCatalog *filtersDB = &(copySpecs->catalogs.filter);
	DatabaseCatalog *targetDB = &(copySpecs->catalogs.target);

	if (!catalog_init(sourceDB) ||
		!catalog_init(filtersDB) ||
		!catalog_init(targetDB))
	{
		/* errors have already been logged */
		return false;
	}

	if (!catalog_register_setup_from_specs(copySpecs))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_open_from_specs opens our SQLite databases for internal catalogs.
 */
bool
catalog_open_from_specs(CopyDataSpec *copySpecs)
{
	DatabaseCatalog *source = &(copySpecs->catalogs.source);
	DatabaseCatalog *filter = &(copySpecs->catalogs.filter);
	DatabaseCatalog *target = &(copySpecs->catalogs.target);

	return catalog_open(source) &&
		   catalog_open(filter) &&
		   catalog_open(target);
}


/*
 * catalog_close_from_specs closes our SQLite databases for internal catalogs.
 */
bool
catalog_close_from_specs(CopyDataSpec *copySpecs)
{
	DatabaseCatalog *source = &(copySpecs->catalogs.source);
	DatabaseCatalog *filter = &(copySpecs->catalogs.filter);
	DatabaseCatalog *target = &(copySpecs->catalogs.target);

	return catalog_close(source) &&
		   catalog_close(filter) &&
		   catalog_close(target);
}


/*
 * catalog_register_setup_from_specs registers the current copySpecs setup.
 */
bool
catalog_register_setup_from_specs(CopyDataSpec *copySpecs)
{
	DatabaseCatalog *sourceDB = &(copySpecs->catalogs.source);

	/*
	 * Fetch and register the catalog setup.
	 *
	 * Because commands such as `pgcopydb list tables` and all might have
	 * fetched parts of the catalogs already, we need to make sure there is no
	 * mismatch between the on-disk catalog setup and the current catalog
	 * setup.
	 *
	 * In case of a mismatch:
	 *
	 *  - if we're running for DATA_SECTION_ALL we can implement cache
	 *    invalidation (drop everything, create everything again, register
	 *    current setup).
	 *
	 * - if we have specs->fetchCatalogs set to true (meaning --force was used)
	 *   we can also implement cache invalidation.
	 *
	 * - in all other cases, we error out with the mismatch information.
	 *
	 * So first prepare the setup information:
	 */
	SafeURI spguri = { 0 };
	SafeURI tpguri = { 0 };

	if (!bareConnectionString(copySpecs->connStrings.source_pguri, &spguri))
	{
		/* errors have already been logged */
		return false;
	}

	if (!bareConnectionString(copySpecs->connStrings.target_pguri, &tpguri))
	{
		/* errors have already been logged */
		return false;
	}

	SourceFilters *filters = &(copySpecs->filters);
	JSON_Value *jsFilters = json_value_init_object();

	if (!filters_as_json(filters, jsFilters))
	{
		/* errors have already been logged */
		return false;
	}

	char *json = json_serialize_to_string(jsFilters);

	/*
	 * Now see if the catalog already have been setup.
	 */
	if (!catalog_setup(sourceDB))
	{
		/* errors have already been logged */
		return false;
	}

	if (sourceDB->setup.id == 0)
	{
		/* catalogs unregistered, register current setup */
		log_notice("Registering catalog setup for "
				   "source \"%s\", target \"%s\", snapshot \"%s\"",
				   spguri.pguri,
				   tpguri.pguri,
				   copySpecs->sourceSnapshot.snapshot);

		if (!catalog_register_setup(sourceDB,
									spguri.pguri,
									tpguri.pguri,
									copySpecs->sourceSnapshot.snapshot,
									copySpecs->splitTablesLargerThan.bytes,
									copySpecs->splitMaxParts,
									json))
		{
			/* errors have already been logged */
			json_free_serialized_string(json);
			return false;
		}
	}
	else
	{
		CatalogSetup *setup = &(sourceDB->setup);

		log_debug("Catalog has been setup for "
				  "source \"%s\", target \"%s\", snapshot \"%s\"",
				  setup->source_pguri,
				  setup->target_pguri,
				  setup->snapshot);

		if (!streq(spguri.pguri, setup->source_pguri))
		{
			log_error("Catalogs at \"%s\" have been setup for "
					  "Postgres source \"%s\" and current source is \"%s\"",
					  sourceDB->dbfile,
					  setup->source_pguri,
					  spguri.pguri);
			return false;
		}

		/*
		 * Not all commands need a target pguri, so we might have registered a
		 * previous setup for the same context but without a target pguri,
		 * which would be NULL in our catalogs at this point.
		 */
		if (setup->target_pguri != NULL && tpguri.pguri != NULL &&
			!streq(tpguri.pguri, setup->target_pguri))
		{
			log_error("Catalogs at \"%s\" have been setup for "
					  "Postgres target \"%s\" and current target is \"%s\"",
					  sourceDB->dbfile,
					  setup->target_pguri,
					  tpguri.pguri);
			return false;
		}

		/* skip comparing snapshots when --not-consistent is used */
		if (copySpecs->consistent)
		{
			if (!streq(copySpecs->sourceSnapshot.snapshot, setup->snapshot))
			{
				log_error("Catalogs at \"%s\" have been setup for "
						  "snapshot \"%s\" and current snapshot is \"%s\"",
						  sourceDB->dbfile,
						  setup->snapshot,
						  copySpecs->sourceSnapshot.snapshot);
				return false;
			}
		}

		/* skip comparing --split-tables-larger-than values unless needed */
		if (copySpecs->section == DATA_SECTION_ALL ||
			copySpecs->section == DATA_SECTION_TABLE_DATA_PARTS)
		{
			CatalogSection *tablePartsDataSection =
				&(sourceDB->sections[DATA_SECTION_TABLE_DATA_PARTS]);

			/* make sure the section has been initialized properly */
			tablePartsDataSection->section = DATA_SECTION_TABLE_DATA_PARTS;

			if (!catalog_section_state(sourceDB, tablePartsDataSection))
			{
				/* errors have already been logged */
				return false;
			}

			/*
			 * Difference in --split-at is only meaningful if table-data cache
			 * has already been populated.
			 */
			if (tablePartsDataSection->fetched &&
				copySpecs->splitTablesLargerThan.bytes !=
				setup->splitTablesLargerThanBytes)
			{
				char bytesPretty[BUFSIZE] = { 0 };

				pretty_print_bytes(bytesPretty,
								   BUFSIZE,
								   setup->splitTablesLargerThanBytes);

				log_debug("setup: %lld (%s)",
						  (long long) setup->splitTablesLargerThanBytes,
						  bytesPretty);

				log_debug("specs: %lld (%s)",
						  (long long) copySpecs->splitTablesLargerThan.bytes,
						  copySpecs->splitTablesLargerThan.bytesPretty);

				log_error("Catalogs at \"%s\" have been setup for "
						  "--split-tables-larger-than \"%s\" "
						  "and current value is \"%s\"",
						  sourceDB->dbfile,
						  bytesPretty,
						  copySpecs->splitTablesLargerThan.bytesPretty);

				return false;
			}

			/*
			 * Difference in --split-max-parts is only meaningful if
			 * table-data cache has already been populated.
			 */
			if (tablePartsDataSection->fetched &&
				copySpecs->splitMaxParts != setup->splitMaxParts)
			{
				log_debug("setup: %d", setup->splitMaxParts);

				log_debug("specs: %d", copySpecs->splitMaxParts);

				log_error("Catalogs at \"%s\" have been setup for "
						  "--split-max-parts \"%d\" "
						  "and current value is \"%d\"",
						  sourceDB->dbfile,
						  setup->splitMaxParts,
						  copySpecs->splitMaxParts);

				return false;
			}
		}

		if (!streq(json, setup->filters))
		{
			log_info("Current filtering setup is: %s", json);
			log_info("Catalog filtering setup is: %s", setup->filters);
			log_error("Catalogs at \"%s\" have been setup for a different "
					  "filtering than the current command, "
					  "see above for details",
					  sourceDB->dbfile);

			return false;
		}
	}

	json_free_serialized_string(json);

	return true;
}


/*
 * catalog_open opens an already initialized catalog database file.
 */
bool
catalog_open(DatabaseCatalog *catalog)
{
	if (!file_exists(catalog->dbfile))
	{
		log_error("Failed to open catalog \"%s\", file does not exists",
				  catalog->dbfile);
		return false;
	}

	return catalog_init(catalog);
}


/*
 * source_catalog_init initializes our internal catalog database file.
 */
bool
catalog_init(DatabaseCatalog *catalog)
{
	if (catalog->db != NULL)
	{
		log_debug("Skipping opening SQLite database \"%s\": already opened",
				  catalog->dbfile);
		return true;
	}

	log_debug("Opening SQLite database \"%s\" with lib version %s",
			  catalog->dbfile,
			  sqlite3_libversion());

	bool createSchema = !file_exists(catalog->dbfile);

	if (sqlite3_open(catalog->dbfile, &(catalog->db)) != SQLITE_OK)
	{
		log_error("Failed to open \"%s\": %s",
				  catalog->dbfile,
				  sqlite3_errmsg(catalog->db));
		return false;
	}

	/*
	 * The source catalog needs a semaphore to serialize concurrent write
	 * access to the SQLite database.
	 */
	if (!catalog_create_semaphore(catalog))
	{
		/* errors have already been logged */
		return false;
	}

	if (createSchema)
	{
		/*
		 * WAL journal_mode is significantly faster for writes and allows
		 * concurrency of readers not blocking writers and vice versa.
		 */
		if (!catalog_set_wal_mode(catalog))
		{
			/* errors have already been logged */
			return false;
		}

		return catalog_create_schema(catalog);
	}

	return true;
}


/*
 * catalog_create_semaphore creates a semaphore to protect concurrent access to
 * the SQLite database that hosts our internal catalogs, allowing sequential
 * access and enforce one-writer-at-a-time.
 */
bool
catalog_create_semaphore(DatabaseCatalog *catalog)
{
	catalog->sema.reentrant = true;

	/*
	 * When we don't have a semId yet (it's zero), create a semaphore. When the
	 * semaphore is non-zero, it's been created already and we can simply use
	 * it: all we need to know is the semId.
	 */
	if (catalog->sema.semId == 0)
	{
		catalog->sema.initValue = 1;

		if (!semaphore_create(&(catalog->sema)))
		{
			log_error("Failed to create the catalog concurrency semaphore");
			return false;
		}
	}

	return true;
}


/*
 * catalog_attach runs the ATTACH SQLite command to attach a catalog b in the
 * already open catalog a, in such a way that it's then possible to query e.g.
 * source.s_table from the filters database.
 */
bool
catalog_attach(DatabaseCatalog *a, DatabaseCatalog *b, const char *name)
{
	char *sqlTmpl = "attach '%s' as %s";
	char buf[BUFSIZE + MAXPGPATH] = { 0 };

	sformat(buf, sizeof(buf), sqlTmpl, b->dbfile, name);

	int rc = sqlite3_exec(a->db, buf, NULL, NULL, NULL);

	if (rc != SQLITE_OK)
	{
		log_error("Failed to attach '%s' as %s", b->dbfile, name);
		log_error("%s", sqlite3_errmsg(a->db));
		return false;
	}

	return true;
}


/*
 * catalog_close closes our internal catalog database file.
 */
bool
catalog_close(DatabaseCatalog *catalog)
{
	/* it's okay to try and close the same catalog twice */
	if (catalog->db == NULL)
	{
		return true;
	}

	if (sqlite3_close(catalog->db) != SQLITE_OK)
	{
		log_error("Failed to close \"%s\":", catalog->dbfile);
		log_error("[SQLite]: %s", sqlite3_errmsg(catalog->db));
		return false;
	}

	catalog->db = NULL;

	return true;
}


/*
 * catalog_create_schema creates the expected schema in the given catalog.
 */
bool
catalog_create_schema(DatabaseCatalog *catalog)
{
	char **createDDLs = NULL;
	int count = 0;

	switch (catalog->type)
	{
		case DATABASE_CATALOG_TYPE_SOURCE:
		{
			createDDLs = sourceDBcreateDDLs;
			count = sizeof(sourceDBcreateDDLs) / sizeof(sourceDBcreateDDLs[0]);
			break;
		}

		case DATABASE_CATALOG_TYPE_FILTER:
		{
			createDDLs = filterDBcreateDDLs;
			count = sizeof(filterDBcreateDDLs) / sizeof(filterDBcreateDDLs[0]);
			break;
		}

		case DATABASE_CATALOG_TYPE_TARGET:
		{
			createDDLs = targetDBcreateDDLs;
			count = sizeof(targetDBcreateDDLs) / sizeof(targetDBcreateDDLs[0]);
			break;
		}

		default:
		{
			log_error("BUG: called catalog_init for unknown type %d",
					  catalog->type);
			return false;
		}
	}

	for (int i = 0; i < count; i++)
	{
		char *ddl = createDDLs[i];

		log_sqlite("catalog_create_schema: %s", ddl);

		int rc = sqlite3_exec(catalog->db, ddl, NULL, NULL, NULL);

		if (rc != SQLITE_OK)
		{
			log_error("Failed to create catalog schema: %s", ddl);
			log_error("%s", sqlite3_errmsg(catalog->db));
			return false;
		}
	}

	return true;
}


/*
 * catalog_drop_schema drops all the catalog schema and data.
 */
bool
catalog_drop_schema(DatabaseCatalog *catalog)
{
	char **dropDDLs = NULL;
	int count = 0;

	switch (catalog->type)
	{
		case DATABASE_CATALOG_TYPE_SOURCE:
		{
			dropDDLs = sourceDBdropDDLs;
			count = sizeof(sourceDBdropDDLs) / sizeof(sourceDBdropDDLs[0]);
			break;
		}

		case DATABASE_CATALOG_TYPE_FILTER:
		{
			dropDDLs = filterDBdropDDLs;
			count = sizeof(filterDBdropDDLs) / sizeof(filterDBdropDDLs[0]);
			break;
		}

		case DATABASE_CATALOG_TYPE_TARGET:
		{
			dropDDLs = targetDBdropDDLs;
			count = sizeof(targetDBdropDDLs) / sizeof(targetDBdropDDLs[0]);
			break;
		}

		default:
		{
			log_error("BUG: called catalog_drop_schema for unknown type %d",
					  catalog->type);
			return false;
		}
	}

	for (int i = 0; i < count; i++)
	{
		char *ddl = dropDDLs[i];

		log_sqlite("catalog_drop_schema: %s", ddl);

		int rc = sqlite3_exec(catalog->db, ddl, NULL, NULL, NULL);

		if (rc != SQLITE_OK)
		{
			log_error("Failed to init catalogs: %s", ddl);
			log_error("%s", sqlite3_errmsg(catalog->db));
			return false;
		}
	}

	return true;
}


/*
 * catalog_set_wal_mode convert given SQLite database to WAL mode
 * (https://www.sqlite.org/pragma.html#pragma_journal_mode).
 *
 * Note: It generates "additional quasi-persistent '-wal' file and '-shm'
 * shared memory file associated with each database"
 * (https://www.sqlite.org/wal.html).
 */
bool
catalog_set_wal_mode(DatabaseCatalog *catalog)
{
	return catalog_execute(catalog, "PRAGMA journal_mode = WAL");
}


/*
 * catalog_begin explicitely begins a SQLite transaction.
 */
bool
catalog_begin(DatabaseCatalog *catalog, bool immediate)
{
	char *sql = immediate ? "BEGIN IMMEDIATE" : "BEGIN";

	log_sqlite("[SQLite] %s", sql);

	int rc = sqlite3_exec(catalog->db, sql, NULL, NULL, NULL);

	if (rc == SQLITE_LOCKED || rc == SQLITE_BUSY)
	{
		ConnectionRetryPolicy retryPolicy = { 0 };

		int maxT = 5;            /* 5s */
		int maxSleepTime = 350;  /* 350ms */
		int baseSleepTime = 10;  /* 10ms */

		(void) pgsql_set_retry_policy(&retryPolicy,
									  maxT,
									  -1, /* unbounded number of attempts */
									  maxSleepTime,
									  baseSleepTime);

		while ((rc == SQLITE_LOCKED || rc == SQLITE_BUSY) &&
			   !pgsql_retry_policy_expired(&retryPolicy))
		{
			int sleepTimeMs =
				pgsql_compute_connection_retry_sleep_time(&retryPolicy);

			log_sqlite("[SQLite %d]: %s, try again in %dms",
					   rc,
					   sqlite3_errstr(rc),
					   sleepTimeMs);

			/* we have milliseconds, pg_usleep() wants microseconds */
			(void) pg_usleep(sleepTimeMs * 1000);

			rc = sqlite3_exec(catalog->db, "BEGIN", NULL, NULL, NULL);
		}
	}

	if (rc != SQLITE_OK)
	{
		log_error("[SQLite] Failed to %s", sql);
		return false;
	}

	return true;
}


/*
 * catalog_commit explicitely commits a SQLite transaction.
 */
bool
catalog_commit(DatabaseCatalog *catalog)
{
	return catalog_execute(catalog, "COMMIT");
}


/*
 * catalog_rollback explicitely rollbacks a SQLite transaction.
 */
bool
catalog_rollback(DatabaseCatalog *catalog)
{
	return catalog_execute(catalog, "ROLLBACK");
}


/*
 * catalog_register_setup registers the setup metadata for this catalog.
 */
bool
catalog_register_setup(DatabaseCatalog *catalog,
					   const char *source_pg_uri,
					   const char *target_pg_uri,
					   const char *snapshot,
					   uint64_t splitTablesLargerThanBytes,
					   int splitMaxParts,
					   const char *filters)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_register_setup: db is NULL");
		return false;
	}

	char *sql =
		"insert into setup("
		"  id, source_pg_uri, target_pg_uri, snapshot, filters, "
		"  split_tables_larger_than, split_max_parts) "
		"values($1, $2, $3, $4, $5, $6, $7)";

	SQLiteQuery query = { 0 };

	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "id", 1, NULL },
		{ BIND_PARAMETER_TYPE_TEXT, "source_pg_uri", 0, (char *) source_pg_uri },
		{ BIND_PARAMETER_TYPE_TEXT, "target_pg_uri", 0, (char *) target_pg_uri },
		{ BIND_PARAMETER_TYPE_TEXT, "snapshot", 0, (char *) snapshot },
		{ BIND_PARAMETER_TYPE_TEXT, "filters", 0, (char *) filters },

		{ BIND_PARAMETER_TYPE_INT64, "split_tables_larger_than",
		  splitTablesLargerThanBytes, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "split_max_parts",
		  splitMaxParts, NULL }
	};

	int count = sizeof(params) / sizeof(params[0]);

	/*
	 * skip splitTableLargerThanBytes, and splitMaxParts when
	 * splitTableLargerThanBytes has not been set.
	 *
	 * skip only splitMaxParts when only splitMaxParts has not been
	 * set.
	 */
	if (splitTablesLargerThanBytes == 0)
	{
		sql =
			"insert into setup("
			"  id, source_pg_uri, target_pg_uri, snapshot, filters) "
			"values($1, $2, $3, $4, $5)";

		count -= 2;
	}
	else if (splitMaxParts == 0)
	{
		sql =
			"insert into setup("
			"  id, source_pg_uri, target_pg_uri, snapshot, filters, "
			"  split_tables_larger_than) "
			"values($1, $2, $3, $4, $5, $6)";

		--count;
	}

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which does not return any row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_setup fetches the registered catalog setup metadata.
 */
bool
catalog_setup(DatabaseCatalog *catalog)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_setup: db is NULL");
		return false;
	}

	SQLiteQuery query = {
		.context = &(catalog->setup),
		.fetchFunction = &catalog_setup_fetch
	};

	char *sql =
		"select id, source_pg_uri, target_pg_uri, snapshot, "
		"       split_tables_larger_than, split_max_parts, filters, "
		"       plugin, slot_name "
		"from setup";

	if (!semaphore_lock(&(catalog->sema)))
	{
		/* errors have already been logged */
		return false;
	}

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* now execute the query, which return exactly one row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	(void) semaphore_unlock(&(catalog->sema));

	return true;
}


/*
 * catalog_update_setup updates the registered catalog setup metadata.
 */
bool
catalog_update_setup(CopyDataSpec *copySpecs)
{
	DatabaseCatalog *catalog = &(copySpecs->catalogs.source);
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_setup: db is NULL");
		return false;
	}

	SafeURI tpguri = { 0 };

	if (!bareConnectionString(copySpecs->connStrings.target_pguri, &tpguri))
	{
		/* errors have already been logged */
		return false;
	}

	SQLiteQuery query = { 0 };

	char *sql =
		"update setup "
		"   set target_pg_uri = $1, "
		"       split_tables_larger_than = $2, "
		"       split_max_parts = $3 "
		" where id = 1";

	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_TEXT, "target_pg_uri", 0, (char *) tpguri.pguri },
		{ BIND_PARAMETER_TYPE_INT64, "split_tables_larger_than",
		  copySpecs->splitTablesLargerThan.bytes, NULL },
		{ BIND_PARAMETER_TYPE_INT, "split_max_parts",
		  copySpecs->splitMaxParts, NULL }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!semaphore_lock(&(catalog->sema)))
	{
		/* errors have already been logged */
		return false;
	}

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which return exactly one row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	(void) semaphore_unlock(&(catalog->sema));

	return true;
}


/*
 * catalog_section_fetch is a SQLiteQuery callback.
 */
bool
catalog_setup_fetch(SQLiteQuery *query)
{
	CatalogSetup *setup = (CatalogSetup *) query->context;

	/*
	 * id
	 */
	setup->id = sqlite3_column_int64(query->ppStmt, 0);

	/*
	 * source_pguri
	 */
	if (sqlite3_column_type(query->ppStmt, 1) != SQLITE_NULL)
	{
		int len = sqlite3_column_bytes(query->ppStmt, 1);
		int bytes = len + 1;

		setup->source_pguri = (char *) calloc(bytes, sizeof(char));

		if (setup->source_pguri == NULL)
		{
			log_fatal(ALLOCATION_FAILED_ERROR);
			return false;
		}

		strlcpy(setup->source_pguri,
				(char *) sqlite3_column_text(query->ppStmt, 1),
				bytes);
	}

	/*
	 * target_pguri
	 */
	if (sqlite3_column_type(query->ppStmt, 2) == SQLITE_NULL)
	{
		setup->target_pguri = NULL;
	}
	else
	{
		int len = sqlite3_column_bytes(query->ppStmt, 2);
		int bytes = len + 1;

		setup->target_pguri = (char *) calloc(bytes, sizeof(char));

		if (setup->target_pguri == NULL)
		{
			log_fatal(ALLOCATION_FAILED_ERROR);
			return false;
		}

		strlcpy(setup->target_pguri,
				(char *) sqlite3_column_text(query->ppStmt, 2),
				bytes);
	}

	/*
	 * snapshot (a string buffer)
	 */
	if (sqlite3_column_type(query->ppStmt, 3) != SQLITE_NULL)
	{
		strlcpy(setup->snapshot,
				(char *) sqlite3_column_text(query->ppStmt, 3),
				sizeof(setup->snapshot));
	}

	/*
	 * split-tables-larger-than
	 */
	setup->splitTablesLargerThanBytes = sqlite3_column_int64(query->ppStmt, 4);

	/*
	 * split-max-parts
	 */
	setup->splitMaxParts = sqlite3_column_int(query->ppStmt, 5);

	/*
	 * filters
	 */
	if (sqlite3_column_type(query->ppStmt, 6) == SQLITE_NULL)
	{
		setup->filters = NULL;
	}
	else
	{
		int len = sqlite3_column_bytes(query->ppStmt, 6);
		int bytes = len + 1;

		setup->filters = (char *) calloc(bytes, sizeof(char));

		if (setup->filters == NULL)
		{
			log_fatal(ALLOCATION_FAILED_ERROR);
			return false;
		}

		strlcpy(setup->filters,
				(char *) sqlite3_column_text(query->ppStmt, 6),
				bytes);
	}

	/*
	 * plugin (a string buffer)
	 */
	if (sqlite3_column_type(query->ppStmt, 7) != SQLITE_NULL)
	{
		strlcpy(setup->plugin,
				(char *) sqlite3_column_text(query->ppStmt, 7),
				sizeof(setup->plugin));
	}

	/*
	 * slot_name (a string buffer)
	 */
	if (sqlite3_column_type(query->ppStmt, 8) != SQLITE_NULL)
	{
		strlcpy(setup->slotName,
				(char *) sqlite3_column_text(query->ppStmt, 8),
				sizeof(setup->slotName));
	}

	return true;
}


/*
 * catalog_setup_replication updates the catalog setup with the information
 * relevant to the logical replication setup. It is meant to be called after
 * having initialized the catalog, once the replication slot has been created,
 * exporting the snapshot.
 */
bool
catalog_setup_replication(DatabaseCatalog *catalog,
						  const char *snapshot,
						  const char *plugin,
						  const char *slotName)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_setup_replication: db is NULL");
		return false;
	}

	char *sql =
		"update setup "
		"   set snapshot = $1, plugin = $2, slot_name = $3 "
		" where id = 1";

	SQLiteQuery query = { .errorOnZeroRows = true };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_TEXT, "snapshot", 0, (char *) snapshot },
		{ BIND_PARAMETER_TYPE_TEXT, "plugin", 0, (char *) plugin },
		{ BIND_PARAMETER_TYPE_TEXT, "slot_name", 0, (char *) slotName }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which does not return any row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_register_section registers that a section has been cached to the
 * internal catalogs.
 */
bool
catalog_register_section(DatabaseCatalog *catalog, TopLevelTiming *timing)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_register_section: db is NULL");
		return false;
	}

	char *sql =
		"insert or replace into section"
		"(name, fetched, start_time_epoch, done_time_epoch, duration) "
		"values($1, $2, $3, $4, $5)";

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_TEXT, "section", 0, (char *) timing->label },
		{ BIND_PARAMETER_TYPE_INT, "fetched", 1, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "start", timing->startTime, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "done", timing->doneTime, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "duration", timing->durationMs, NULL }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which does not return any row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_section_state sets the fetched boolean to the catalog value.
 */
bool
catalog_section_state(DatabaseCatalog *catalog, CatalogSection *section)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_section_state: db is NULL");
		return false;
	}

	SQLiteQuery query = {
		.context = section,
		.fetchFunction = &catalog_section_fetch
	};

	char *sql = "select name, fetched, duration from section where name = $1";

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_TEXT, "name", 0,
		  CopyDataSectionToString(section->section) }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which return exactly one row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_section_fetch is a SQLiteQuery callback.
 */
bool
catalog_section_fetch(SQLiteQuery *query)
{
	CatalogSection *section = (CatalogSection *) query->context;

	strlcpy(section->name,
			(char *) sqlite3_column_text(query->ppStmt, 1),
			sizeof(section->name));

	section->fetched = sqlite3_column_int(query->ppStmt, 1) == 1;

	section->durationMs = sqlite3_column_int64(query->ppStmt, 2);

	return true;
}


/*
 * catalog_total_duration loops over a catalog section array and compute the
 * total duration in milliseconds.
 */
bool
catalog_total_duration(DatabaseCatalog *catalog)
{
	catalog->totalDurationMs = 0;

	for (int i = 1; i < DATA_SECTION_COUNT; i++)
	{
		CatalogSection *s = &(catalog->sections[i]);

		catalog->totalDurationMs += s->durationMs;
	}

	return true;
}


/*
 * CopyDataSectionToString returns a string representation of a section.
 */
char *
CopyDataSectionToString(CopyDataSection section)
{
	switch (section)
	{
		case DATA_SECTION_DATABASE_PROPERTIES:
		{
			return "database-properties";
		}

		case DATA_SECTION_COLLATIONS:
		{
			return "collations";
		}

		case DATA_SECTION_EXTENSIONS:
		{
			return "extension";
		}

		case DATA_SECTION_SCHEMA:
		{
			return "schema";
		}

		case DATA_SECTION_TABLE_DATA:
		{
			return "table-data";
		}

		case DATA_SECTION_TABLE_DATA_PARTS:
		{
			return "table-data-parts";
		}

		case DATA_SECTION_SET_SEQUENCES:
		{
			return "set-sequences";
		}

		case DATA_SECTION_INDEXES:
		{
			return "indexes";
		}

		case DATA_SECTION_CONSTRAINTS:
		{
			return "constraints";
		}

		case DATA_SECTION_DEPENDS:
		{
			return "pg_depend";
		}

		case DATA_SECTION_FILTERS:
		{
			return "filters";
		}

		case DATA_SECTION_BLOBS:
		{
			return "large-objects";
		}

		case DATA_SECTION_VACUUM:
		{
			return "vacuum";
		}

		case DATA_SECTION_ALL:
		{
			return "all";
		}

		case DATA_SECTION_NAMESPACES:
		{
			return "namespaces";
		}

		case DATA_SECTION_NONE:
		default:
		{
			log_error("BUG: CopyDataSectionToString unknown section %d", section);
			return NULL;
		}
	}

	/* keep compiler happy */
	return "unknown";
}


/*
 * catalog_add_s_matview INSERTs a SourceTable to our matview internal catalogs
 * database.
 */
bool
catalog_add_s_matview(DatabaseCatalog *catalog, SourceTable *table)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_add_s_matview: db is NULL");
		return false;
	}

	char *sql =
		"insert into s_matview("
		"  oid, qname, nspname, relname, restore_list_name, "
		"  exclude_data) "
		"values($1, $2, $3, $4, $5, $6)";

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "oid", table->oid, NULL },
		{ BIND_PARAMETER_TYPE_TEXT, "qname", 0, table->qname },
		{ BIND_PARAMETER_TYPE_TEXT, "nspname", 0, table->nspname },
		{ BIND_PARAMETER_TYPE_TEXT, "relname", 0, table->relname },

		{ BIND_PARAMETER_TYPE_TEXT, "restore_list_name", 0,
		  table->restoreListName },

		{ BIND_PARAMETER_TYPE_INT, "exclude_data",
		  table->excludeData ? 1 : 0, NULL },
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which does not return any row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_lookup_s_matview_by_oid fetches a s_matview entry from catalog.
 */
bool
catalog_lookup_s_matview_by_oid(DatabaseCatalog *catalog,
								CatalogMatView *result,
								uint32_t oid)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_lookup_filter_by_oid: db is NULL");
		return false;
	}

	char *sql =
		"  select oid, nspname, relname, restore_list_name, exclude_data"
		"    from s_matview "
		"   where oid = $1 ";

	SQLiteQuery query = {
		.context = result,
		.fetchFunction = &catalog_s_matview_fetch
	};

	if (!semaphore_lock(&(catalog->sema)))
	{
		/* errors have already been logged */
		return false;
	}

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* bind our parameters now */
	BindParam params[1] = {
		{ BIND_PARAMETER_TYPE_INT64, "oid", oid, NULL },
	};

	if (!catalog_sql_bind(&query, params, 1))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* now execute the query, which return exactly one row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	(void) semaphore_unlock(&(catalog->sema));

	return true;
}


/*
 * catalog_s_matview_fetch fetches a CatalogMatview entry from a SQLite ppStmt
 * result set.
 */
bool
catalog_s_matview_fetch(SQLiteQuery *query)
{
	CatalogMatView *entry = (CatalogMatView *) query->context;

	/* cleanup the memory area before re-use */
	bzero(entry, sizeof(CatalogMatView));

	entry->oid = sqlite3_column_int64(query->ppStmt, 0);

	if (sqlite3_column_type(query->ppStmt, 1) != SQLITE_NULL)
	{
		strlcpy(entry->nspname,
				(char *) sqlite3_column_text(query->ppStmt, 1),
				sizeof(entry->nspname));
	}

	if (sqlite3_column_type(query->ppStmt, 2) != SQLITE_NULL)
	{
		strlcpy(entry->relname,
				(char *) sqlite3_column_text(query->ppStmt, 2),
				sizeof(entry->relname));
	}

	if (sqlite3_column_type(query->ppStmt, 3) != SQLITE_NULL)
	{
		strlcpy(entry->restoreListName,
				(char *) sqlite3_column_text(query->ppStmt, 3),
				sizeof(entry->restoreListName));
	}

	entry->excludeData = sqlite3_column_int64(query->ppStmt, 4) == 1;

	return true;
}


/*
 * catalog_add_s_table INSERTs a SourceTable to our internal catalogs database.
 */
bool
catalog_add_s_table(DatabaseCatalog *catalog, SourceTable *table)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_add_s_table: db is NULL");
		return false;
	}

	char *sql =
		"insert into s_table("
		"  oid, qname, nspname, relname, amname, restore_list_name, "
		"  relpages, reltuples, exclude_data, part_key) "
		"values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "oid", table->oid, NULL },
		{ BIND_PARAMETER_TYPE_TEXT, "qname", 0, table->qname },
		{ BIND_PARAMETER_TYPE_TEXT, "nspname", 0, table->nspname },
		{ BIND_PARAMETER_TYPE_TEXT, "relname", 0, table->relname },
		{ BIND_PARAMETER_TYPE_TEXT, "amname", 0, table->amname },

		{ BIND_PARAMETER_TYPE_TEXT, "restore_list_name", 0,
		  table->restoreListName },

		{ BIND_PARAMETER_TYPE_INT64, "relpages", table->relpages, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "reltuples", table->reltuples, NULL },

		{ BIND_PARAMETER_TYPE_INT, "exclude_data",
		  table->excludeData ? 1 : 0, NULL },

		{ BIND_PARAMETER_TYPE_TEXT, "part_key", 0, table->partKey }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which does not return any row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	/* now add the attributes */
	if (!catalog_add_attributes(catalog, table))
	{
		log_error("Failed to add table %s attributes, see above for details",
				  table->qname);
		return false;
	}

	return true;
}


/*
 * catalog_add_attributes INSERTs a SourceTable attributes array to our
 * internal catalogs database (s_attr).
 */
bool
catalog_add_attributes(DatabaseCatalog *catalog, SourceTable *table)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_add_attributes: db is NULL");
		return false;
	}

	char *sql =
		"insert into s_attr("
		"oid, attnum, attypid, attname, attisprimary, attisgenerated)"
		"values($1, $2, $3, $4, $5, $6)";

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	for (int i = 0; i < table->attributes.count; i++)
	{
		SourceTableAttribute *attr = &(table->attributes.array[i]);

		BindParam params[] = {
			{ BIND_PARAMETER_TYPE_INT64, "oid", table->oid, NULL },
			{ BIND_PARAMETER_TYPE_INT64, "attnum", attr->attnum, NULL },
			{ BIND_PARAMETER_TYPE_INT64, "atttypid", attr->atttypid, NULL },
			{ BIND_PARAMETER_TYPE_TEXT, "attname", 0, attr->attname },

			{ BIND_PARAMETER_TYPE_INT, "attisprimary",
			  attr->attisprimary ? 1 : 0, NULL },

			{ BIND_PARAMETER_TYPE_INT, "attisgenerated",
			  attr->attisgenerated ? 1 : 0, NULL }
		};

		int count = sizeof(params) / sizeof(params[0]);

		if (!catalog_sql_bind(&query, params, count))
		{
			/* errors have already been logged */
			return false;
		}

		/* now execute the query, which does not return any row */
		if (!catalog_sql_execute(&query))
		{
			/* errors have already been logged */
			return false;
		}
	}

	/* and finalize the query */
	if (!catalog_sql_finalize(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_add_s_table_part INSERTs a SourceTableParts to our internal catalogs
 * database (s_table_parts).
 */
bool
catalog_add_s_table_part(DatabaseCatalog *catalog, SourceTable *table)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_add_s_table_part: db is NULL");
		return false;
	}

	char *sql =
		"insert into s_table_part(oid, partnum, partcount, min, max, count)"
		"values($1, $2, $3, $4, $5, $6)";

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	SourceTableParts *part = &(table->partition);

	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "oid", table->oid, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "partnum", part->partNumber, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "partcount", part->partCount, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "min", part->min, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "max", part->max, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "count", part->count, NULL },
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which does not return any row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_add_s_table INSERTs a SourceTable to our internal catalogs database.
 */
bool
catalog_add_s_table_chksum(DatabaseCatalog *catalog,
						   SourceTable *table,
						   TableChecksum *srcChk,
						   TableChecksum *dstChk)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_add_s_table_chksum: db is NULL");
		return false;
	}

	char *sql =
		"insert into s_table_chksum("
		"  oid, srcrowcount, srcsum, dstrowcount, dstsum)"
		"values($1, $2, $3, $4, $5)";

	if (!semaphore_lock(&(catalog->sema)))
	{
		/* errors have already been logged */
		return false;
	}

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "oid", table->oid, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "srcrowcount", srcChk->rowcount, NULL },
		{ BIND_PARAMETER_TYPE_TEXT, "srcsum", 0, srcChk->checksum },
		{ BIND_PARAMETER_TYPE_INT64, "dstrowcount", dstChk->rowcount, NULL },
		{ BIND_PARAMETER_TYPE_TEXT, "dstsum", 0, dstChk->checksum }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* now execute the query, which does not return any row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	(void) semaphore_unlock(&(catalog->sema));

	return true;
}


/*
 * catalog_add_s_table_size inserts a SourceTableSize to our internal catalogs database.
 */
bool
catalog_add_s_table_size(DatabaseCatalog *catalog,
						 SourceTableSize *tableSize)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_add_s_table_size: db is NULL");
		return false;
	}

	char *sql =
		"insert into s_table_size("
		"  oid, bytes, bytes_pretty)"
		"values($1, $2, $3)";

	if (!semaphore_lock(&(catalog->sema)))
	{
		/* errors have already been logged */
		return false;
	}

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "oid", tableSize->oid, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "bytes", tableSize->bytes, NULL },
		{ BIND_PARAMETER_TYPE_TEXT, "bytes_pretty", 0, tableSize->bytesPretty },
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* now execute the query, which does not return any row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	(void) semaphore_unlock(&(catalog->sema));

	return true;
}


/*
 * catalog_delete_s_table_chksum_all implements cache invalidation for pgcopydb
 * compare data.
 */
bool
catalog_delete_s_table_chksum_all(DatabaseCatalog *catalog)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_delete_s_table_chksum_all: db is NULL");
		return false;
	}

	char *sql = "delete from s_table_chksum";

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which does not return any row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_stats fetches statistics about the objects we have in our catalog.
 */
bool
catalog_stats(DatabaseCatalog *catalog, CatalogStats *stats)
{
	if (!catalog_s_table_stats(catalog, &(stats->table)))
	{
		/* errors have already been logged */
		return false;
	}

	if (!catalog_count_objects(catalog, &(stats->count)))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_s_table_stats fetches statistics about the SourceTable list we have
 * in our catalog.
 */
bool
catalog_s_table_stats(DatabaseCatalog *catalog, CatalogTableStats *stats)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_s_table_stats: db is NULL");
		return false;
	}

	char *sql =
		"select count(t.oid) as count, "
		"       count(p.oid) as countSplits, "
		"       sum(p.partcount) as countParts, "
		"       sum(ts.bytes) as totalBytes, "
		"       sum(reltuples) as totalTuples "
		"  from s_table t "
		"       left join "
		"         ("
		"             select oid, count(*) as partcount "
		"               from s_table_part "
		"           group by oid"
		"         ) p "
		"        on p.oid = t.oid"
		"       left join s_table_size ts on ts.oid = t.oid ";

	SQLiteQuery query = {
		.context = stats,
		.fetchFunction = &catalog_s_table_stats_fetch
	};

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which return exactly one row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_s_table_stats_fetch is a SQLiteQuery callback.
 */
bool
catalog_s_table_stats_fetch(SQLiteQuery *query)
{
	CatalogTableStats *stats = (CatalogTableStats *) query->context;

	stats->count = sqlite3_column_int64(query->ppStmt, 0);
	stats->countSplits = sqlite3_column_int64(query->ppStmt, 1);
	stats->countParts = sqlite3_column_int64(query->ppStmt, 2);
	stats->totalBytes = sqlite3_column_int64(query->ppStmt, 3);
	stats->totalTuples = sqlite3_column_int64(query->ppStmt, 4);

	(void) pretty_print_bytes(stats->bytesPretty, BUFSIZE, stats->totalBytes);
	(void) pretty_print_count(stats->relTuplesPretty, BUFSIZE, stats->totalTuples);

	return true;
}


/*
 * catalog_count_objects returns how many objects were added to the catalogs.
 */
bool
catalog_count_objects(DatabaseCatalog *catalog, CatalogCounts *count)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_count_objects: db is NULL");
		return false;
	}

	char *sql = NULL;

	switch (catalog->type)
	{
		case DATABASE_CATALOG_TYPE_SOURCE:
		{
			sql =
				"select (select count(1) as rel from s_table), "
				"       (select count(1) as idx from s_index), "
				"       (select count(1) as con from s_constraint),"
				"       (select count(1) as seq from s_seq),"
				"       0 as rol,"
				"       (select count(1) as dat from s_database),"
				"       0 as nsp,"
				"       0 as ext,"
				"       0 as colls,"
				"       0 as pg_depend";
			break;
		}

		case DATABASE_CATALOG_TYPE_FILTER:
		{
			sql =
				"select (select count(1) as rel from s_table), "
				"       (select count(1) as idx from s_index), "
				"       (select count(1) as con from s_constraint),"
				"       (select count(1) as seq from s_seq),"
				"       0 as rol,"
				"       0 as dat,"
				"       (select count(1) as nsp from s_namespace),"
				"       (select count(1) as ext from s_extension),"
				"       (select count(1) as col from s_coll),"
				"       (select count(1) as dep from s_depend)";
			break;
		}

		case DATABASE_CATALOG_TYPE_TARGET:
		{
			sql =
				"select (select count(1) as rel from s_table), "
				"       (select count(1) as idx from s_index), "
				"       (select count(1) as con from s_constraint),"
				"       0 as seq,"
				"       (select count(1) as rol from s_role),"
				"       0 as dat,"
				"       (select count(1) as nsp from s_namespace),"
				"       0 as ext,"
				"       0 as colls,"
				"       0 as pg_depend";
			break;
		}

		default:
		{
			log_error("BUG: called catalog_count_objects for unknown type %d",
					  catalog->type);
			return false;
		}
	}

	SQLiteQuery query = {
		.context = count,
		.fetchFunction = &catalog_count_fetch
	};

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which return exactly one row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_count_fetch fetches a CatalogIndexCount from a query ppStmt
 * result.
 */
bool
catalog_count_fetch(SQLiteQuery *query)
{
	CatalogCounts *count = (CatalogCounts *) query->context;

	count->tables = sqlite3_column_int64(query->ppStmt, 0);
	count->indexes = sqlite3_column_int64(query->ppStmt, 1);
	count->constraints = sqlite3_column_int64(query->ppStmt, 2);
	count->sequences = sqlite3_column_int64(query->ppStmt, 3);

	count->roles = sqlite3_column_int64(query->ppStmt, 4);
	count->databases = sqlite3_column_int64(query->ppStmt, 5);
	count->namespaces = sqlite3_column_int64(query->ppStmt, 6);
	count->extensions = sqlite3_column_int64(query->ppStmt, 7);
	count->colls = sqlite3_column_int64(query->ppStmt, 8);
	count->depends = sqlite3_column_int64(query->ppStmt, 9);

	return true;
}


/*
 * catalog_lookup_s_table fetches a SourceTable entry from our catalogs.
 */
bool
catalog_lookup_s_table(DatabaseCatalog *catalog,
					   uint32_t oid,
					   int partNumber,
					   SourceTable *table)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_lookup_s_table: db is NULL");
		return false;
	}

	if (!semaphore_lock(&(catalog->sema)))
	{
		/* errors have already been logged */
		return false;
	}

	SQLiteQuery query = {
		.context = table,
		.fetchFunction = &catalog_s_table_fetch
	};

	if (partNumber > 0)
	{
		char *sql =
			"  select t.oid, qname, nspname, relname, amname, restore_list_name, "
			"         relpages, reltuples, ts.bytes, ts.bytes_pretty, "
			"         exclude_data, part_key, "
			"         p.partcount as partcount, p.partnum, p.min, p.max "
			"    from s_table t "
			"         join s_table_part p "
			"           on t.oid = p.oid "
			"          and p.partnum = $1"
			"       left join s_table_size ts on ts.oid = t.oid "
			"   where t.oid = $2 ";

		if (!catalog_sql_prepare(db, sql, &query))
		{
			/* errors have already been logged */
			(void) semaphore_unlock(&(catalog->sema));
			return false;
		}

		/* bind our parameters now */
		BindParam params[] = {
			{ BIND_PARAMETER_TYPE_INT64, "partnum", partNumber, NULL },
			{ BIND_PARAMETER_TYPE_INT64, "oid", oid, NULL }
		};

		int count = sizeof(params) / sizeof(params[0]);

		if (!catalog_sql_bind(&query, params, count))
		{
			/* errors have already been logged */
			(void) semaphore_unlock(&(catalog->sema));
			return false;
		}
	}
	else
	{
		char *sql =
			"  select t.oid, qname, nspname, relname, amname, restore_list_name, "
			"         relpages, reltuples, ts.bytes, ts.bytes_pretty, "
			"         exclude_data, part_key, "
			"         count(p.oid) as partcount "
			"    from s_table t left join s_table_part p on t.oid = p.oid"
			"       left join s_table_size ts on ts.oid = t.oid "
			"   where t.oid = $1 ";

		if (!catalog_sql_prepare(db, sql, &query))
		{
			/* errors have already been logged */
			(void) semaphore_unlock(&(catalog->sema));
			return false;
		}

		/* bind our parameters now */
		BindParam params[] = {
			{ BIND_PARAMETER_TYPE_INT64, "oid", oid, NULL }
		};

		int count = sizeof(params) / sizeof(params[0]);

		if (!catalog_sql_bind(&query, params, count))
		{
			/* errors have already been logged */
			(void) semaphore_unlock(&(catalog->sema));
			return false;
		}
	}

	/* now execute the query, which return exactly one row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	(void) semaphore_unlock(&(catalog->sema));

	return true;
}


/*
 * catalog_lookup_s_table_by_name fetches a SourceTable from our catalogs.
 */
bool
catalog_lookup_s_table_by_name(DatabaseCatalog *catalog,
							   const char *nspname,
							   const char *relname,
							   SourceTable *table)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_s_table_stats: db is NULL");
		return false;
	}

	char *sql =
		"  select t.oid, qname, nspname, relname, amname, restore_list_name, "
		"         relpages, reltuples, ts.bytes, ts.bytes_pretty, "
		"         exclude_data, part_key, "
		"         p.partcount, 0 as partnum, 0 as min, 0 as max "
		"    from s_table t "
		"         left join "
		"         ("
		"             select oid, count(*) as partcount "
		"               from s_table_part "
		"           group by oid"
		"         ) p "
		"        on p.oid = t.oid"
		"       left join s_table_size ts on ts.oid = t.oid "
		"   where nspname = $1 and relname = $2 ";

	SQLiteQuery query = {
		.context = table,
		.fetchFunction = &catalog_s_table_fetch
	};

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_TEXT, "nspname", 0, (char *) nspname },
		{ BIND_PARAMETER_TYPE_TEXT, "relname", 0, (char *) relname },
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which return exactly one row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_lookup_s_attr_by_name fetches a SourceTable from our catalogs.
 */
bool
catalog_lookup_s_attr_by_name(DatabaseCatalog *catalog,
							  uint32_t reloid,
							  const char *attname,
							  SourceTableAttribute *attribute)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_lookup_s_attr_by_name: db is NULL");
		return false;
	}

	char *sql =
		"  select attnum, attypid, attname, attisprimary, attisgenerated "
		"    from s_attr "
		"   where oid = $1 and attname = $2";

	SQLiteQuery query = {
		.context = attribute,
		.fetchFunction = &catalog_s_attr_fetch
	};

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "oid", reloid, NULL },
		{ BIND_PARAMETER_TYPE_TEXT, "attname", 0, (char *) attname },
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which return exactly one row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_delete_s_table deletes an s_table entry for the given oid.
 */
bool
catalog_delete_s_table(DatabaseCatalog *catalog,
					   const char *nspname,
					   const char *relname)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: Failed to initialize s_table iterator: db is NULL");
		return false;
	}

	char *sql = "delete from s_table where nspname = $1 and relname = $2";

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_TEXT, "nspname", 0, (char *) nspname },
		{ BIND_PARAMETER_TYPE_TEXT, "relname", 0, (char *) relname }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which does not return any row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_iter_s_table iterates over the list of tables in our catalogs.
 */
bool
catalog_iter_s_table(DatabaseCatalog *catalog,
					 void *context,
					 SourceTableIterFun *callback)
{
	SourceTableIterator *iter =
		(SourceTableIterator *) calloc(1, sizeof(SourceTableIterator));

	iter->catalog = catalog;

	if (!catalog_iter_s_table_init(iter))
	{
		/* errors have already been logged */
		return false;
	}

	for (;;)
	{
		if (!catalog_iter_s_table_next(iter))
		{
			/* errors have already been logged */
			return false;
		}

		SourceTable *table = iter->table;

		if (table == NULL)
		{
			if (!catalog_iter_s_table_finish(iter))
			{
				/* errors have already been logged */
				return false;
			}

			break;
		}

		/* now call the provided callback */
		if (!(*callback)(context, table))
		{
			log_error("Failed to iterate over list of tables, "
					  "see above for details");
			return false;
		}
	}


	return true;
}


/*
 * catalog_iter_s_table_nopk iterates over the list of tables that don't have a
 * Primary Key in our catalogs.
 */
bool
catalog_iter_s_table_nopk(DatabaseCatalog *catalog,
						  void *context,
						  SourceTableIterFun *callback)
{
	SourceTableIterator *iter =
		(SourceTableIterator *) calloc(1, sizeof(SourceTableIterator));

	iter->catalog = catalog;

	if (!catalog_iter_s_table_nopk_init(iter))
	{
		/* errors have already been logged */
		return false;
	}

	for (;;)
	{
		if (!catalog_iter_s_table_next(iter))
		{
			/* errors have already been logged */
			return false;
		}

		SourceTable *table = iter->table;

		if (table == NULL)
		{
			if (!catalog_iter_s_table_finish(iter))
			{
				/* errors have already been logged */
				return false;
			}

			break;
		}

		/* now call the provided callback */
		if (!(*callback)(context, table))
		{
			log_error("Failed to iterate over list of tables, "
					  "see above for details");
			return false;
		}
	}


	return true;
}


/*
 * catalog_iter_s_table_init initializes an Interator over our catalog of
 * SourceTable entries.
 */
bool
catalog_iter_s_table_init(SourceTableIterator *iter)
{
	sqlite3 *db = iter->catalog->db;

	if (db == NULL)
	{
		log_error("BUG: Failed to initialize s_table iterator: db is NULL");
		return false;
	}

	iter->table = (SourceTable *) calloc(1, sizeof(SourceTable));

	if (iter->table == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	char *sql =
		"  select t.oid, qname, nspname, relname, amname, restore_list_name, "
		"         relpages, reltuples, ts.bytes, ts.bytes_pretty, "
		"         exclude_data, part_key, "
		"         coalesce(p.partcount, 0) as partcount, "
		"         coalesce(p.partnum, 0) as partnum, "
		"         coalesce(p.min, 0) as min, coalesce(p.max, 0) as max, "
		"         c.srcrowcount, c.srcsum, c.dstrowcount, c.dstsum, "
		"         sum(s.duration), sum(s.bytes) "
		"    from s_table t "
		"         left join s_table_part p on p.oid = t.oid "
		"         left join s_table_chksum c on c.oid = t.oid "
		"         left join summary s on s.tableoid = t.oid "
		"         left join s_table_size ts on ts.oid = t.oid "
		"group by t.oid "
		"order by ts.bytes desc";

	SQLiteQuery *query = &(iter->query);

	query->context = iter->table;
	query->fetchFunction = &catalog_s_table_fetch;

	if (!catalog_sql_prepare(db, sql, query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_iter_s_table_init initializes an Interator over our catalog of
 * SourceTable entries.
 */
bool
catalog_iter_s_table_nopk_init(SourceTableIterator *iter)
{
	sqlite3 *db = iter->catalog->db;

	if (db == NULL)
	{
		log_error("BUG: Failed to initialize s_table iterator: db is NULL");
		return false;
	}

	iter->table = (SourceTable *) calloc(1, sizeof(SourceTable));

	if (iter->table == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	char *sql =
		"  select t.oid, qname, nspname, relname, amname, restore_list_name, "
		"         relpages, reltuples, ts.bytes, ts.bytes_pretty, "
		"         exclude_data, part_key, "
		"         (select count(1) from s_table_part p where p.oid = t.oid) "
		"    from s_table t join join s_attr a on a.oid = t.oid "
		"       left join s_table_size ts on ts.oid = t.oid "
		"group by t.oid "
		"  having sum(a.attisprimary) = 0 "
		"order by bytes desc";

	SQLiteQuery *query = &(iter->query);

	query->context = iter->table;
	query->fetchFunction = &catalog_s_table_fetch;

	if (!catalog_sql_prepare(db, sql, query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_iter_s_table_next fetches the next SourceTable entry in our catalogs.
 */
bool
catalog_iter_s_table_next(SourceTableIterator *iter)
{
	SQLiteQuery *query = &(iter->query);

	int rc = catalog_sql_step(query);

	if (rc == SQLITE_DONE)
	{
		iter->table = NULL;

		return true;
	}

	if (rc != SQLITE_ROW)
	{
		log_error("Failed to step through statement: %s", query->sql);
		log_error("[SQLite] %s", sqlite3_errmsg(query->db));
		return false;
	}

	return catalog_s_table_fetch(query);
}


/*
 * catalog_s_table_fetch fetches a SourceTable entry from a SQLite ppStmt
 * result set.
 */
bool
catalog_s_table_fetch(SQLiteQuery *query)
{
	SourceTable *table = (SourceTable *) query->context;

	/* cleanup the memory area before re-use */
	bzero(table, sizeof(SourceTable));

	table->oid = sqlite3_column_int64(query->ppStmt, 0);

	if (sqlite3_column_type(query->ppStmt, 1) != SQLITE_NULL)
	{
		strlcpy(table->qname,
				(char *) sqlite3_column_text(query->ppStmt, 1),
				sizeof(table->qname));
	}

	if (sqlite3_column_type(query->ppStmt, 2) != SQLITE_NULL)
	{
		strlcpy(table->nspname,
				(char *) sqlite3_column_text(query->ppStmt, 2),
				sizeof(table->nspname));
	}

	if (sqlite3_column_type(query->ppStmt, 3) != SQLITE_NULL)
	{
		strlcpy(table->relname,
				(char *) sqlite3_column_text(query->ppStmt, 3),
				sizeof(table->relname));
	}

	if (sqlite3_column_type(query->ppStmt, 4) != SQLITE_NULL)
	{
		strlcpy(table->amname,
				(char *) sqlite3_column_text(query->ppStmt, 4),
				sizeof(table->amname));
	}

	if (sqlite3_column_type(query->ppStmt, 5) != SQLITE_NULL)
	{
		strlcpy(table->restoreListName,
				(char *) sqlite3_column_text(query->ppStmt, 5),
				sizeof(table->restoreListName));
	}

	table->relpages = sqlite3_column_int64(query->ppStmt, 6);
	table->reltuples = sqlite3_column_int64(query->ppStmt, 7);
	table->bytes = sqlite3_column_int64(query->ppStmt, 8);

	if (sqlite3_column_type(query->ppStmt, 9) != SQLITE_NULL)
	{
		strlcpy(table->bytesPretty,
				(char *) sqlite3_column_text(query->ppStmt, 9),
				sizeof(table->bytesPretty));
	}

	table->excludeData = sqlite3_column_int64(query->ppStmt, 10) == 1;

	if (sqlite3_column_type(query->ppStmt, 11) != SQLITE_NULL)
	{
		strlcpy(table->partKey,
				(char *) sqlite3_column_text(query->ppStmt, 11),
				sizeof(table->partKey));
	}

	table->partition.partCount = sqlite3_column_int64(query->ppStmt, 12);

	/*
	 * The main iterator query returns partition count, whereas the catalog
	 * fetch query, which is given a table oid, then returns partNumber, min,
	 * max, and count values.
	 */
	int cols = sqlite3_column_count(query->ppStmt);

	/* partition information from s_table_part */
	if (cols >= 16)
	{
		table->partition.partNumber = sqlite3_column_int64(query->ppStmt, 13);
		table->partition.min = sqlite3_column_int64(query->ppStmt, 14);
		table->partition.max = sqlite3_column_int64(query->ppStmt, 15);
	}

	/* checksum information from s_table_chksum */
	if (cols >= 20)
	{
		table->sourceChecksum.rowcount =
			sqlite3_column_int64(query->ppStmt, 16);

		if (sqlite3_column_type(query->ppStmt, 17) != SQLITE_NULL)
		{
			strlcpy(table->sourceChecksum.checksum,
					(char *) sqlite3_column_text(query->ppStmt, 17),
					sizeof(table->sourceChecksum.checksum));
		}

		table->targetChecksum.rowcount =
			sqlite3_column_int64(query->ppStmt, 18);

		if (sqlite3_column_type(query->ppStmt, 19) != SQLITE_NULL)
		{
			strlcpy(table->targetChecksum.checksum,
					(char *) sqlite3_column_text(query->ppStmt, 19),
					sizeof(table->targetChecksum.checksum));
		}
	}

	/* summary information from s_table_parts_done */
	if (cols == 22)
	{
		table->durationMs = sqlite3_column_int64(query->ppStmt, 20);
		table->bytesTransmitted = sqlite3_column_int64(query->ppStmt, 21);
	}

	return true;
}


/*
 * catalog_iter_s_table_finish cleans-up the internal memory used for the
 * iteration.
 */
bool
catalog_iter_s_table_finish(SourceTableIterator *iter)
{
	SQLiteQuery *query = &(iter->query);

	if (!catalog_sql_finalize(query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_iter_s_table_part iterates over the list of a table partitions in
 * our catalogs.
 */
bool
catalog_iter_s_table_parts(DatabaseCatalog *catalog,
						   uint32_t oid,
						   void *context,
						   SourceTablePartsIterFun *callback)
{
	SourceTablePartsIterator *iter =
		(SourceTablePartsIterator *) calloc(1, sizeof(SourceTablePartsIterator));

	iter->catalog = catalog;
	iter->oid = oid;

	if (!catalog_iter_s_table_part_init(iter))
	{
		/* errors have already been logged */
		return false;
	}

	for (;;)
	{
		if (!catalog_iter_s_table_part_next(iter))
		{
			/* errors have already been logged */
			return false;
		}

		SourceTableParts *part = iter->part;

		if (part == NULL)
		{
			if (!catalog_iter_s_table_part_finish(iter))
			{
				/* errors have already been logged */
				return false;
			}

			break;
		}

		/* now call the provided callback */
		if (!(*callback)(context, part))
		{
			log_error("Failed to iterate over list of tables, "
					  "see above for details");
			return false;
		}
	}


	return true;
}


/*
 * catalog_iter_s_table_part_init initializes an Interator over our catalog of
 * SourceTable entries.
 */
bool
catalog_iter_s_table_part_init(SourceTablePartsIterator *iter)
{
	sqlite3 *db = iter->catalog->db;

	if (db == NULL)
	{
		log_error("BUG: Failed to initialize s_table iterator: db is NULL");
		return false;
	}

	iter->part = (SourceTableParts *) calloc(1, sizeof(SourceTableParts));

	if (iter->part == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	char *sql =
		"  select partnum, partcount, min, max, count "
		"    from s_table_part "
		"   where oid = $1 "
		"order by partnum";

	SQLiteQuery *query = &(iter->query);

	query->context = iter->part;
	query->fetchFunction = &catalog_s_table_part_fetch;

	if (!catalog_sql_prepare(db, sql, query))
	{
		/* errors have already been logged */
		return false;
	}

	BindParam params[1] = {
		{ BIND_PARAMETER_TYPE_INT64, "oid", iter->oid, NULL }
	};

	if (!catalog_sql_bind(query, params, 1))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_iter_s_table_part_next fetches the next SourceTable entry in our catalogs.
 */
bool
catalog_iter_s_table_part_next(SourceTablePartsIterator *iter)
{
	SQLiteQuery *query = &(iter->query);

	int rc = catalog_sql_step(query);

	if (rc == SQLITE_DONE)
	{
		iter->part = NULL;

		return true;
	}

	if (rc != SQLITE_ROW)
	{
		log_error("Failed to step through statement: %s", query->sql);
		log_error("[SQLite] %s", sqlite3_errmsg(query->db));
		return false;
	}

	return catalog_s_table_part_fetch(query);
}


/*
 * catalog_s_table_part_fetch fetches a SourceTableParts entry from a SQLite
 * ppStmt result set.
 */
bool
catalog_s_table_part_fetch(SQLiteQuery *query)
{
	SourceTableParts *part = (SourceTableParts *) query->context;

	/* cleanup the memory area before re-use */
	bzero(part, sizeof(SourceTableParts));

	part->partNumber = sqlite3_column_int64(query->ppStmt, 0);
	part->partCount = sqlite3_column_int64(query->ppStmt, 1);
	part->min = sqlite3_column_int64(query->ppStmt, 2);
	part->max = sqlite3_column_int64(query->ppStmt, 3);
	part->count = sqlite3_column_int64(query->ppStmt, 4);

	return true;
}


/*
 * catalog_iter_s_table_part_finish cleans-up the internal memory used for the
 * iteration.
 */
bool
catalog_iter_s_table_part_finish(SourceTablePartsIterator *iter)
{
	SQLiteQuery *query = &(iter->query);

	if (!catalog_sql_finalize(query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_s_table_attrlist fetches the attributes of a table as a single
 * C-string, using ', ' as a separator.
 */
bool
catalog_s_table_attrlist(DatabaseCatalog *catalog, SourceTable *table)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_s_table_attrlist: db is NULL");
		return false;
	}

	char *sql =
		" select group_concat(attname order by attnum, ', ') "
		"       filter (where not attisgenerated) "
		"  from s_attr "
		" where oid = $1";

	SQLiteQuery query = {
		.context = table,
		.fetchFunction = &catalog_s_table_fetch_attrlist
	};

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* bind our parameters now */
	BindParam params[1] = {
		{ BIND_PARAMETER_TYPE_INT64, "oid", table->oid, NULL }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which return exactly one row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_s_table_fetch_attrlist fetches a SourceTable attrlist from a SQLite
 * query result.
 */
bool
catalog_s_table_fetch_attrlist(SQLiteQuery *query)
{
	SourceTable *table = (SourceTable *) query->context;

	/* the default empty attribute list is an empty string */
	table->attrList = "";

	if (sqlite3_column_type(query->ppStmt, 0) != SQLITE_NULL)
	{
		int len = sqlite3_column_bytes(query->ppStmt, 0);
		int bytes = len + 1;

		table->attrList = (char *) calloc(bytes, sizeof(char));

		if (table->attrList == NULL)
		{
			log_fatal(ALLOCATION_FAILED_ERROR);
			return false;
		}

		strlcpy(table->attrList,
				(char *) sqlite3_column_text(query->ppStmt, 0),
				bytes);
	}

	return true;
}


/*
 * catalog_s_table_fetch_attrs fetches the table SourceTableAttribute array
 * from our s_attr catalog.
 */
bool
catalog_s_table_fetch_attrs(DatabaseCatalog *catalog, SourceTable *table)
{
	SourceTableAttrsIterator *iter =
		(SourceTableAttrsIterator *) calloc(1,
											sizeof(SourceTableAttrsIterator));

	if (iter == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	iter->catalog = catalog;
	iter->table = table;

	if (!catalog_iter_s_table_attrs_init(iter))
	{
		/* errors have already been logged */
		return false;
	}

	while (!iter->done)
	{
		if (!catalog_iter_s_table_attrs_next(iter))
		{
			/* errors have already been logged */
			return false;
		}
	}

	if (!catalog_iter_s_table_attrs_finish(iter))
	{
		/* errors have already been logged */
		return false;
	}


	return true;
}


/*
 * catalog_iter_s_table_attrs_init initializes an Interator over our catalog of
 * SourceTableAttributes entries.
 */
bool
catalog_iter_s_table_attrs_init(SourceTableAttrsIterator *iter)
{
	sqlite3 *db = iter->catalog->db;

	if (db == NULL)
	{
		log_error("BUG: Failed to initialize s_table iterator: db is NULL");
		return false;
	}

	SourceTable *table = iter->table;

	if (iter->table == NULL)
	{
		log_error("BUG: Failed to initialize s_table iterator: table is NULL");
		return false;
	}

	char *sql =
		"  select count(*) over(order by attnum) as num, "
		"         count(*) over() as count, "
		"         attnum, attypid, attname, attisprimary, attisgenerated "
		"    from s_attr "
		"   where oid = $1 "
		"order by attnum";

	SQLiteQuery *query = &(iter->query);

	query->context = iter->table;
	query->fetchFunction = &catalog_s_table_attrs_fetch;

	if (!catalog_sql_prepare(db, sql, query))
	{
		/* errors have already been logged */
		return false;
	}

	/* bind our parameters now */
	BindParam params[1] = {
		{ BIND_PARAMETER_TYPE_INT64, "oid", table->oid, NULL }
	};

	if (!catalog_sql_bind(query, params, 1))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_iter_s_table_attrs_next fetches the next SourceTable entry in our
 * catalogs.
 */
bool
catalog_iter_s_table_attrs_next(SourceTableAttrsIterator *iter)
{
	SQLiteQuery *query = &(iter->query);

	int rc = catalog_sql_step(query);

	if (rc == SQLITE_DONE)
	{
		iter->done = true;
		return true;
	}

	if (rc != SQLITE_ROW)
	{
		log_error("Failed to step through statement: %s", query->sql);
		log_error("[SQLite] %s", sqlite3_errmsg(query->db));
		return false;
	}

	return catalog_s_table_attrs_fetch(query);
}


/*
 * catalog_iter_s_table_attrs_finish cleans-up the internal memory used for the
 * iteration.
 */
bool
catalog_iter_s_table_attrs_finish(SourceTableAttrsIterator *iter)
{
	SQLiteQuery *query = &(iter->query);

	if (!catalog_sql_finalize(query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_s_table_attrs_fetch  is a SQLiteQuery callback.
 */
bool
catalog_s_table_attrs_fetch(SQLiteQuery *query)
{
	SourceTable *table = (SourceTable *) query->context;

	int num = sqlite3_column_int(query->ppStmt, 0);
	int count = sqlite3_column_int(query->ppStmt, 1);

	if (num == 1)
	{
		table->attributes.count = count;
		table->attributes.array =
			(SourceTableAttribute *) calloc(count,
											sizeof(SourceTableAttribute));

		if (table->attributes.array == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}
	}

	SourceTableAttribute *attr = &(table->attributes.array[num - 1]);

	attr->attnum = sqlite3_column_int(query->ppStmt, 2);
	attr->atttypid = sqlite3_column_int64(query->ppStmt, 3);

	strlcpy(attr->attname,
			(char *) sqlite3_column_text(query->ppStmt, 4),
			sizeof(attr->attname));

	attr->attisprimary = sqlite3_column_int(query->ppStmt, 5) == 1;
	attr->attisgenerated = sqlite3_column_int(query->ppStmt, 6) == 1;

	return true;
}


/*
 * catalog_s_attr_fetch  is a SQLiteQuery callback.
 */
bool
catalog_s_attr_fetch(SQLiteQuery *query)
{
	SourceTableAttribute *attr = (SourceTableAttribute *) query->context;

	attr->attnum = sqlite3_column_int64(query->ppStmt, 0);
	attr->atttypid = sqlite3_column_int64(query->ppStmt, 1);

	strlcpy(attr->attname,
			(char *) sqlite3_column_text(query->ppStmt, 2),
			sizeof(attr->attname));

	attr->attisprimary = sqlite3_column_int(query->ppStmt, 3) == 1;
	attr->attisgenerated = sqlite3_column_int(query->ppStmt, 4) == 1;

	return true;
}


/*
 * catalog_s_table_fetch_attrs fetches the table SourceTableAttribute array
 * from our s_attr catalog.
 */
bool
catalog_s_table_count_attrs(DatabaseCatalog *catalog, SourceTable *table)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_s_table_count_attrs: db is NULL");
		return false;
	}

	char *sql = "select count(1) from s_attr where oid = $1";

	SQLiteQuery query = {
		.context = table,
		.fetchFunction = &catalog_s_table_count_attrs_fetch
	};

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* bind our parameters now */
	BindParam params[1] = {
		{ BIND_PARAMETER_TYPE_INT64, "oid", table->oid, NULL }
	};

	if (!catalog_sql_bind(&query, params, 1))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which return exactly one row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_s_table_count_attrs_fetch  is a SQLiteQuery callback.
 */
bool
catalog_s_table_count_attrs_fetch(SQLiteQuery *query)
{
	SourceTable *table = (SourceTable *) query->context;

	int count = sqlite3_column_int(query->ppStmt, 0);

	table->attributes.count = count;
	table->attributes.array = NULL;

	return true;
}


/*
 * catalog_add_s_index INSERTs a SourceIndex to our internal catalogs database.
 */
bool
catalog_add_s_index(DatabaseCatalog *catalog, SourceIndex *index)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_add_s_index: db is NULL");
		return false;
	}

	char *sql =
		"insert into s_index("
		"  oid, qname, nspname, relname, restore_list_name, tableoid, "
		"  isprimary, isunique, columns, sql) "
		"values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "oid", index->indexOid, NULL },
		{ BIND_PARAMETER_TYPE_TEXT, "qname", 0, index->indexQname },
		{ BIND_PARAMETER_TYPE_TEXT, "nspname", 0, index->indexNamespace },
		{ BIND_PARAMETER_TYPE_TEXT, "relname", 0, index->indexRelname },

		{ BIND_PARAMETER_TYPE_TEXT, "restore_list_name", 0,
		  index->indexRestoreListName },

		{ BIND_PARAMETER_TYPE_INT64, "tableoid", index->tableOid, NULL },

		{ BIND_PARAMETER_TYPE_INT, "isprimary", index->isPrimary ? 1 : 0, NULL },
		{ BIND_PARAMETER_TYPE_INT, "isunique", index->isUnique ? 1 : 0, NULL },

		{ BIND_PARAMETER_TYPE_TEXT, "columns", 0, index->indexColumns },
		{ BIND_PARAMETER_TYPE_TEXT, "sql", 0, index->indexDef }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which does not return any row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_add_s_constraint INSERTs a SourceIndex constraint to our internal
 * catalogs database.
 */
bool
catalog_add_s_constraint(DatabaseCatalog *catalog, SourceIndex *index)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_add_s_index: db is NULL");
		return false;
	}

	char *sql =
		"insert into s_constraint("
		"  oid, conname, indexoid, condeferrable, condeferred, sql)"
		"values($1, $2, $3, $4, $5, $6)";

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "oid", index->constraintOid, NULL },
		{ BIND_PARAMETER_TYPE_TEXT, "conname", 0, index->constraintName },
		{ BIND_PARAMETER_TYPE_INT64, "indexoid", index->indexOid, NULL },

		{ BIND_PARAMETER_TYPE_INT, "condeferable",
		  index->condeferrable ? 1 : 0, NULL },
		{ BIND_PARAMETER_TYPE_INT, "condeffered",
		  index->condeferred ? 1 : 0, NULL },

		{ BIND_PARAMETER_TYPE_TEXT, "sql", 0, index->constraintDef }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which does not return any row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_lookup_s_index fetches a SourceIndex entry from our catalogs.
 */
bool
catalog_lookup_s_index(DatabaseCatalog *catalog, uint32_t oid, SourceIndex *index)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_lookup_s_index: db is NULL");
		return false;
	}

	char *sql =
		"  select i.oid, i.qname, i.nspname, i.relname, i.restore_list_name, "
		"         i.tableoid, t.qname, t.nspname, t.relname, "
		"         isprimary, isunique, columns, i.sql, "
		"         c.oid as constraintoid, conname, "
		"         condeferrable, condeferred, c.sql as condef"
		"    from s_index i "
		"         join s_table t on t.oid = i.tableoid "
		"         left join s_constraint c on c.indexoid = i.oid"
		"   where i.oid = $1 ";

	if (!semaphore_lock(&(catalog->sema)))
	{
		/* errors have already been logged */
		return false;
	}

	SQLiteQuery query = {
		.context = index,
		.fetchFunction = &catalog_s_index_fetch
	};

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* bind our parameters now */
	BindParam params[1] = {
		{ BIND_PARAMETER_TYPE_INT64, "oid", oid, NULL },
	};

	if (!catalog_sql_bind(&query, params, 1))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* now execute the query, which return exactly one row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	(void) semaphore_unlock(&(catalog->sema));

	return true;
}


/*
 * catalog_lookup_s_index_by_name fetches a SourceIndex entry from our catalogs.
 */
bool
catalog_lookup_s_index_by_name(DatabaseCatalog *catalog,
							   const char *nspname,
							   const char *relname,
							   SourceIndex *index)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_lookup_s_index_by_name: db is NULL");
		return false;
	}

	char *sql =
		"  select i.oid, i.qname, i.nspname, i.relname, i.restore_list_name, "
		"         i.tableoid, t.qname, t.nspname, t.relname, "
		"         isprimary, isunique, columns, i.sql, "
		"         c.oid as constraintoid, conname, "
		"         condeferrable, condeferred, c.sql as condef"
		"    from s_index i "
		"         join s_table t on t.oid = i.tableoid "
		"         left join s_constraint c on c.indexoid = i.oid"
		"   where i.nspname = $1 and i.relname = $2 ";

	if (!semaphore_lock(&(catalog->sema)))
	{
		/* errors have already been logged */
		return false;
	}

	SQLiteQuery query = {
		.context = index,
		.fetchFunction = &catalog_s_index_fetch
	};

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_TEXT, "nspname", 0, (char *) nspname },
		{ BIND_PARAMETER_TYPE_TEXT, "relname", 0, (char *) relname }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* now execute the query, which return exactly one row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	(void) semaphore_unlock(&(catalog->sema));

	return true;
}


/*
 * catalog_s_index_fetch fetches a SourceIndex entry from a SQLite ppStmt
 * result set.
 */
bool
catalog_s_index_fetch(SQLiteQuery *query)
{
	SourceIndex *index = (SourceIndex *) query->context;

	/* cleanup the memory area before re-use */
	bzero(index, sizeof(SourceIndex));

	index->indexOid = sqlite3_column_int64(query->ppStmt, 0);

	strlcpy(index->indexQname,
			(char *) sqlite3_column_text(query->ppStmt, 1),
			sizeof(index->indexQname));

	strlcpy(index->indexNamespace,
			(char *) sqlite3_column_text(query->ppStmt, 2),
			sizeof(index->indexNamespace));

	strlcpy(index->indexRelname,
			(char *) sqlite3_column_text(query->ppStmt, 3),
			sizeof(index->indexRelname));

	strlcpy(index->indexRestoreListName,
			(char *) sqlite3_column_text(query->ppStmt, 4),
			sizeof(index->indexRestoreListName));

	index->tableOid = sqlite3_column_int64(query->ppStmt, 5);

	strlcpy(index->tableQname,
			(char *) sqlite3_column_text(query->ppStmt, 6),
			sizeof(index->tableQname));

	strlcpy(index->tableNamespace,
			(char *) sqlite3_column_text(query->ppStmt, 7),
			sizeof(index->tableNamespace));

	strlcpy(index->tableRelname,
			(char *) sqlite3_column_text(query->ppStmt, 8),
			sizeof(index->tableRelname));

	index->isPrimary = sqlite3_column_int(query->ppStmt, 9) == 1;
	index->isUnique = sqlite3_column_int(query->ppStmt, 10) == 1;

	if (sqlite3_column_type(query->ppStmt, 11) != SQLITE_NULL)
	{
		int len = sqlite3_column_bytes(query->ppStmt, 11);
		int bytes = len + 1;

		index->indexColumns = (char *) calloc(bytes, sizeof(char));

		if (index->indexColumns == NULL)
		{
			log_fatal(ALLOCATION_FAILED_ERROR);
			return false;
		}

		strlcpy(index->indexColumns,
				(char *) sqlite3_column_text(query->ppStmt, 11),
				bytes);
	}

	if (sqlite3_column_type(query->ppStmt, 12) != SQLITE_NULL)
	{
		int len = sqlite3_column_bytes(query->ppStmt, 12);
		int bytes = len + 1;

		index->indexDef = (char *) calloc(bytes, sizeof(char));

		if (index->indexDef == NULL)
		{
			log_fatal(ALLOCATION_FAILED_ERROR);
			return false;
		}

		strlcpy(index->indexDef,
				(char *) sqlite3_column_text(query->ppStmt, 12),
				bytes);
	}

	/* constraint */
	if (sqlite3_column_type(query->ppStmt, 13) != SQLITE_NULL)
	{
		index->constraintOid = sqlite3_column_int64(query->ppStmt, 13);

		strlcpy(index->constraintName,
				(char *) sqlite3_column_text(query->ppStmt, 14),
				sizeof(index->constraintName));

		index->condeferrable = sqlite3_column_int(query->ppStmt, 15) == 1;
		index->condeferred = sqlite3_column_int(query->ppStmt, 16) == 1;


		if (sqlite3_column_type(query->ppStmt, 17) != SQLITE_NULL)
		{
			int len = sqlite3_column_bytes(query->ppStmt, 17);
			int bytes = len + 1;

			index->constraintDef = (char *) calloc(bytes, sizeof(char));

			if (index->constraintDef == NULL)
			{
				log_fatal(ALLOCATION_FAILED_ERROR);
				return false;
			}

			strlcpy(index->constraintDef,
					(char *) sqlite3_column_text(query->ppStmt, 17),
					bytes);
		}
	}

	return true;
}


/*
 * catalog_iter_s_index iterates over the list of indexes in our catalogs.
 */
bool
catalog_iter_s_index(DatabaseCatalog *catalog,
					 void *context,
					 SourceIndexIterFun *callback)
{
	SourceIndexIterator *iter =
		(SourceIndexIterator *) calloc(1, sizeof(SourceIndexIterator));

	if (iter == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	iter->catalog = catalog;

	if (!catalog_iter_s_index_init(iter))
	{
		/* errors have already been logged */
		return false;
	}

	for (;;)
	{
		if (!catalog_iter_s_index_next(iter))
		{
			/* errors have already been logged */
			return false;
		}

		SourceIndex *index = iter->index;

		if (index == NULL)
		{
			if (!catalog_iter_s_index_finish(iter))
			{
				/* errors have already been logged */
				return false;
			}

			break;
		}

		/* now call the provided callback */
		if (!(*callback)(context, index))
		{
			log_error("Failed to iterate over list of indexes, "
					  "see above for details");
			return false;
		}
	}


	return true;
}


/*
 * catalog_iter_s_index iterates over the list of indexes in our catalogs.
 */
bool
catalog_iter_s_index_table(DatabaseCatalog *catalog,
						   const char *nspname,
						   const char *relname,
						   void *context,
						   SourceIndexIterFun *callback)
{
	SourceIndexIterator *iter =
		(SourceIndexIterator *) calloc(1, sizeof(SourceIndexIterator));

	iter->catalog = catalog;
	iter->nspname = nspname;
	iter->relname = relname;

	if (!semaphore_lock(&(catalog->sema)))
	{
		/* errors have already been logged */
		return false;
	}

	if (!catalog_iter_s_index_table_init(iter))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	for (;;)
	{
		if (!catalog_iter_s_index_next(iter))
		{
			/* errors have already been logged */
			(void) semaphore_unlock(&(catalog->sema));
			return false;
		}

		SourceIndex *index = iter->index;

		if (index == NULL)
		{
			if (!catalog_iter_s_index_finish(iter))
			{
				/* errors have already been logged */
				(void) semaphore_unlock(&(catalog->sema));
				return false;
			}

			break;
		}

		/* now call the provided callback */
		if (!(*callback)(context, index))
		{
			log_error("Failed to iterate over list of indexes, "
					  "see above for details");
			(void) semaphore_unlock(&(catalog->sema));
			return false;
		}
	}

	(void) semaphore_unlock(&(catalog->sema));

	return true;
}


/*
 * catalog_iter_s_index_init initializes an Interator over our catalog of
 * SourceIndex entries.
 */
bool
catalog_iter_s_index_init(SourceIndexIterator *iter)
{
	sqlite3 *db = iter->catalog->db;

	if (db == NULL)
	{
		log_error("BUG: Failed to initialize s_index iterator: db is NULL");
		return false;
	}

	iter->index = (SourceIndex *) calloc(1, sizeof(SourceIndex));

	if (iter->index == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	char *sql =
		"  select i.oid, i.qname, i.nspname, i.relname, i.restore_list_name, "
		"         i.tableoid, t.qname, t.nspname, t.relname, "
		"         isprimary, isunique, columns, i.sql, "
		"         c.oid as constraintoid, conname, "
		"         condeferrable, condeferred, c.sql as condef"
		"    from s_index i "
		"         join s_table t on t.oid = i.tableoid "
		"		  left join s_table_size ts on ts.oid = i.tableoid"
		"         left join s_constraint c on c.indexoid = i.oid "
		"order by ts.bytes desc, t.oid";

	SQLiteQuery *query = &(iter->query);

	query->context = iter->index;
	query->fetchFunction = &catalog_s_index_fetch;

	if (!catalog_sql_prepare(db, sql, query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_iter_s_index_init initializes an Interator over our catalog of
 * SourceIndex entries.
 */
bool
catalog_iter_s_index_table_init(SourceIndexIterator *iter)
{
	sqlite3 *db = iter->catalog->db;

	if (db == NULL)
	{
		log_error("BUG: Failed to initialize s_index iterator: db is NULL");
		return false;
	}

	iter->index = (SourceIndex *) calloc(1, sizeof(SourceIndex));

	if (iter->index == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	char *sql =
		"  select i.oid, i.qname, i.nspname, i.relname, i.restore_list_name, "
		"         i.tableoid, t.qname, t.nspname, t.relname, "
		"         isprimary, isunique, columns, i.sql, "
		"         c.oid as constraintoid, conname, "
		"         condeferrable, condeferred, c.sql as condef"
		"    from s_index i "
		"         join s_table t on t.oid = i.tableoid "
		"         left join s_constraint c on c.indexoid = i.oid "
		"   where t.nspname = $1 and t.relname = $2";

	SQLiteQuery *query = &(iter->query);

	query->context = iter->index;
	query->fetchFunction = &catalog_s_index_fetch;

	if (!catalog_sql_prepare(db, sql, query))
	{
		/* errors have already been logged */
		return false;
	}

	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_TEXT, "nspname", 0, (char *) iter->nspname },
		{ BIND_PARAMETER_TYPE_TEXT, "relname", 0, (char *) iter->relname }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(query, params, count))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_iter_s_index_next fetches the next SourceIndex entry in our catalogs.
 */
bool
catalog_iter_s_index_next(SourceIndexIterator *iter)
{
	SQLiteQuery *query = &(iter->query);

	int rc = catalog_sql_step(query);

	if (rc == SQLITE_DONE)
	{
		iter->index = NULL;

		return true;
	}

	if (rc != SQLITE_ROW)
	{
		log_error("Failed to step through statement: %s", query->sql);
		log_error("[SQLite] %s", sqlite3_errmsg(query->db));
		return false;
	}

	return catalog_s_index_fetch(query);
}


/*
 * catalog_iter_s_index_finish cleans-up the internal memory used for the
 * iteration.
 */
bool
catalog_iter_s_index_finish(SourceIndexIterator *iter)
{
	SQLiteQuery *query = &(iter->query);

	/* in case we finish before reaching the DONE step */
	if (iter->index != NULL)
	{
		iter->index = NULL;
	}

	if (!catalog_sql_finalize(query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_s_table_fetch_attrs fetches the table SourceTableAttribute array
 * from our s_attr catalog.
 */
bool
catalog_s_table_count_indexes(DatabaseCatalog *catalog, SourceTable *table)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_s_table_count_indexes: db is NULL");
		return false;
	}

	char *sql =
		"select count(1) as indexes, "
		"       count(c.oid) as constraints "
		"  from s_index i "
		"       left join s_constraint c on c.indexoid = i.oid "
		" where tableoid = $1";

	if (!semaphore_lock(&(catalog->sema)))
	{
		/* errors have already been logged */
		return false;
	}

	SQLiteQuery query = {
		.context = table,
		.fetchFunction = &catalog_s_table_count_indexes_fetch
	};

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* bind our parameters now */
	BindParam params[1] = {
		{ BIND_PARAMETER_TYPE_INT64, "oid", table->oid, NULL }
	};

	if (!catalog_sql_bind(&query, params, 1))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* now execute the query, which return exactly one row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	(void) semaphore_unlock(&(catalog->sema));

	return true;
}


/*
 * catalog_s_table_count_indexes_fetch  is a SQLiteQuery callback.
 */
bool
catalog_s_table_count_indexes_fetch(SQLiteQuery *query)
{
	SourceTable *table = (SourceTable *) query->context;

	table->indexCount = sqlite3_column_int64(query->ppStmt, 0);
	table->constraintCount = sqlite3_column_int64(query->ppStmt, 1);

	return true;
}


/*
 * catalog_delete_s_index_all DELETE all the indexes registered in the given
 * database catalog.
 */
bool
catalog_delete_s_index_all(DatabaseCatalog *catalog)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_delete_s_index_all: db is NULL");
		return false;
	}

	char *sql = "delete from s_index";

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which does not return any row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_delete_s_index_table DELETE all the indexes registered in the given
 * database catalog for the given table.
 */
bool
catalog_delete_s_index_table(DatabaseCatalog *catalog,
							 const char *nspname,
							 const char *relname)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: Failed to initialize s_index iterator: db is NULL");
		return false;
	}

	char *sql =
		"delete from s_index "
		" where tableoid = "
		"       ("
		"        select oid "
		"          from s_table "
		"         where nspname = $1 and relname = $2"
		"        )";

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_TEXT, "nspname", 0, (char *) nspname },
		{ BIND_PARAMETER_TYPE_TEXT, "relname", 0, (char *) relname }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which does not return any row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_add_s_seq INSERTs a SourceSequence to our internal catalogs database.
 */
bool
catalog_add_s_seq(DatabaseCatalog *catalog, SourceSequence *seq)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_add_s_seq: db is NULL");
		return false;
	}

	char *sql =
		"insert into s_seq("
		"  oid, ownedby, attrelid, attroid, "
		"  qname, nspname, relname, restore_list_name)"
		"values($1, $2, $3, $4, $5, $6, $7, $8)";

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "oid", seq->oid, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "ownedby", seq->ownedby, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "attrelid", seq->attrelid, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "attroid", seq->attroid, NULL },

		{ BIND_PARAMETER_TYPE_TEXT, "qname", 0, seq->qname },
		{ BIND_PARAMETER_TYPE_TEXT, "nspname", 0, seq->nspname },
		{ BIND_PARAMETER_TYPE_TEXT, "relname", 0, seq->relname },

		{ BIND_PARAMETER_TYPE_TEXT, "restore_list_name", 0,
		  seq->restoreListName }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which does not return any row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_update_sequence_values UPDATEs a SourceSequence lastValue and
 * isCalled parameters in our catalogs.
 */
bool
catalog_update_sequence_values(DatabaseCatalog *catalog, SourceSequence *seq)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_update_sequence_values: db is NULL");
		return false;
	}

	char *sql =
		"update s_seq "
		"   set last_value = $1, isCalled = $2 "
		" where nspname = $3 and relname = $4";

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "last_alue", seq->lastValue, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "isCalled", seq->isCalled ? 1 : 0, NULL },
		{ BIND_PARAMETER_TYPE_TEXT, "nspname", 0, seq->nspname },
		{ BIND_PARAMETER_TYPE_TEXT, "relname", 0, seq->relname }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which does not return any row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * Updates the 'relpages' field of the 's_table' table in the database catalog with the given values.
 */
bool
catalog_update_s_table_relpages(DatabaseCatalog *catalog, SourceTable *sourceTable)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_update_s_table_relpages: db is NULL");
		return false;
	}

	char *sql =
		"update s_table "
		"   set relpages = $1 "
		" where oid = $2";

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "relpages", sourceTable->relpages, NULL },
		{ BIND_PARAMETER_TYPE_TEXT, "oid", sourceTable->oid, NULL },
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which does not return any row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_lookup_s_seq_by_name fetches a SourceSeq from our catalogs.
 */
bool
catalog_lookup_s_seq_by_name(DatabaseCatalog *catalog,
							 const char *nspname,
							 const char *relname,
							 SourceSequence *seq)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_s_seq_stats: db is NULL");
		return false;
	}

	char *sql =
		"  select oid, ownedby, attrelid, attroid, "
		"         qname, nspname, relname, restore_list_name, "
		"         last_value, isCalled "
		"    from s_seq "
		"   where nspname = $1 and relname = $2 ";

	SQLiteQuery query = {
		.context = seq,
		.fetchFunction = &catalog_s_seq_fetch
	};

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_TEXT, "nspname", 0, (char *) nspname },
		{ BIND_PARAMETER_TYPE_TEXT, "relname", 0, (char *) relname },
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which return exactly one row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_iter_s_seq iterates over the list of sequences in our catalogs.
 */
bool
catalog_iter_s_seq(DatabaseCatalog *catalog,
				   void *context,
				   SourceSequenceIterFun *callback)
{
	SourceSeqIterator *iter =
		(SourceSeqIterator *) calloc(1, sizeof(SourceSeqIterator));

	iter->catalog = catalog;

	if (!catalog_iter_s_seq_init(iter))
	{
		/* errors have already been logged */
		return false;
	}

	for (;;)
	{
		if (!catalog_iter_s_seq_next(iter))
		{
			/* errors have already been logged */
			return false;
		}

		SourceSequence *seq = iter->seq;

		if (seq == NULL)
		{
			if (!catalog_iter_s_seq_finish(iter))
			{
				/* errors have already been logged */
				return false;
			}

			break;
		}

		/* now call the provided callback */
		if (!(*callback)(context, seq))
		{
			log_error("Failed to iterate over list of seqs, "
					  "see above for details");
			return false;
		}
	}


	return true;
}


/*
 * catalog_iter_s_seq_init initializes an Interator over our catalog of
 * SourceSequence entries.
 */
bool
catalog_iter_s_seq_init(SourceSeqIterator *iter)
{
	sqlite3 *db = iter->catalog->db;

	if (db == NULL)
	{
		log_error("BUG: Failed to initialize s_seq iterator: db is NULL");
		return false;
	}

	iter->seq = (SourceSequence *) calloc(1, sizeof(SourceSequence));

	if (iter->seq == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	char *sql =
		"  select oid, ownedby, attrelid, attroid, "
		"         qname, nspname, relname, restore_list_name, "
		"         last_value, isCalled "
		"    from s_seq "
		"order by nspname, relname";

	SQLiteQuery *query = &(iter->query);

	query->context = iter->seq;
	query->fetchFunction = &catalog_s_seq_fetch;

	if (!catalog_sql_prepare(db, sql, query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_iter_s_seq_next fetches the next SourceSequence entry in our catalogs.
 */
bool
catalog_iter_s_seq_next(SourceSeqIterator *iter)
{
	SQLiteQuery *query = &(iter->query);

	int rc = catalog_sql_step(query);

	if (rc == SQLITE_DONE)
	{
		iter->seq = NULL;

		return true;
	}

	if (rc != SQLITE_ROW)
	{
		log_error("Failed to step through statement: %s", query->sql);
		log_error("[SQLite] %s", sqlite3_errmsg(query->db));
		return false;
	}

	return catalog_s_seq_fetch(query);
}


/*
 * catalog_s_seq_fetch fetches a SourceSequence entry from a SQLite ppStmt
 * result set.
 */
bool
catalog_s_seq_fetch(SQLiteQuery *query)
{
	SourceSequence *seq = (SourceSequence *) query->context;

	/* cleanup the memory area before re-use */
	bzero(seq, sizeof(SourceSequence));

	seq->oid = sqlite3_column_int64(query->ppStmt, 0);
	seq->ownedby = sqlite3_column_int64(query->ppStmt, 1);
	seq->attrelid = sqlite3_column_int64(query->ppStmt, 2);
	seq->attroid = sqlite3_column_int64(query->ppStmt, 3);

	strlcpy(seq->qname,
			(char *) sqlite3_column_text(query->ppStmt, 4),
			sizeof(seq->qname));

	strlcpy(seq->nspname,
			(char *) sqlite3_column_text(query->ppStmt, 5),
			sizeof(seq->nspname));

	strlcpy(seq->relname,
			(char *) sqlite3_column_text(query->ppStmt, 6),
			sizeof(seq->relname));

	strlcpy(seq->restoreListName,
			(char *) sqlite3_column_text(query->ppStmt, 7),
			sizeof(seq->restoreListName));

	seq->lastValue = sqlite3_column_int64(query->ppStmt, 8);
	seq->isCalled = sqlite3_column_int(query->ppStmt, 9);

	return true;
}


/*
 * catalog_iter_s_seq_finish cleans-up the internal memory used for the
 * iteration.
 */
bool
catalog_iter_s_seq_finish(SourceSeqIterator *iter)
{
	SQLiteQuery *query = &(iter->query);

	/* in case we finish before reaching the DONE step */
	if (iter->seq != NULL)
	{
		iter->seq = NULL;
	}

	if (!catalog_sql_finalize(query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_prepare_filter prepares our filter Hash-Table, that used to be an
 * in-memory only thing, and now is a SQLite table with indexes, so that it can
 * spill to disk when we have giant database catalogs to take care of.
 */
bool
catalog_prepare_filter(DatabaseCatalog *catalog,
					   bool skipExtensions,
					   bool skipCollations)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_prepare_filter: db is NULL");
		return false;
	}

	char *sql =
		"insert into filter(oid, restore_list_name, kind) "

		"     select oid, restore_list_name, 'table' "
		"       from s_table "

		/*
		 * This is only for materialized views. Materialized view refresh
		 * filtering is done with the help of s_matview table on source
		 * catalog.
		 */
		"  union all "

		"	 select oid, restore_list_name, 'matview' "
		"	   from s_matview"

		"  union all "

		"     select oid, restore_list_name, 'index' "
		"       from s_index "

		"  union all "

		/* at the moment we lack restore names for constraints */
		"     select oid, NULL as restore_list_name, 'constraint' "
		"       from s_constraint "

		/*
		 * Filtering-out sequences works with the following 3 Archive Catalog
		 * entry kinds:
		 *
		 *  - SEQUENCE, matched by sequence oid
		 *  - SEQUENCE OWNED BY, matched by sequence restore name
		 *  - DEFAULT, matched by attribute oid
		 *
		 * In some cases we want to create the sequence, but we might want to
		 * skip the SEQUENCE OWNED BY statement, because we didn't actually
		 * create the owner table.
		 *
		 * In those cases we will find the sequence both in the catalogs of
		 * objects we want to migrate, and also in the list of objects we want
		 * to skip. The catalog entry typically has seq->ownedby !=
		 * seq->attrelid, where the ownedby table is skipped from the migration
		 * because of the filtering.
		 */
		"  union all "

		/*
		 * When we find the sequence in our source catalog selection, then we
		 * still create it and refrain to add the sequence Oid to our hash
		 * table here.
		 */
		"     select distinct s.oid, NULL as restore_list_name, 'sequence' "
		"       from s_seq s "
		"      where not exists"
		"            (select 1 from source.s_seq ss where ss.oid = s.oid)"

		/*
		 * Only filter-out the SEQUENCE OWNED BY when our catalog selection
		 * does not contain the target table.
		 */
		"  union all "

		"     select NULL as oid, restore_list_name, 'sequence owned by' "
		"       from ( "
		"              select distinct s.restore_list_name "
		"                from s_seq s "
		"               where not exists"
		"                     (select 1 "
		"                        from source.s_seq ss "
		"                       where ss.oid = s.oid) "
		"                and not exists"
		"                    (select 1 "
		"                       from source.s_table st "
		"                      where st.oid = s.ownedby) "
		"            ) as seqownedby "

		/*
		 * Also add pg_attribute.oid when it's not null (non-zero here). This
		 * takes care of the DEFAULT entries in the pg_dump Archive Catalog,
		 * and these entries target the attroid directly.
		 */
		"  union all "

		"     select distinct s.attroid, s.restore_list_name, 'default' "
		"       from s_seq s "
		"      where s.attroid > 0";

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which does not return any row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * In some cases with sequences we might want to skip adding a dependency
	 * in our hash table here. See the previous discussion for details.
	 */
	char *s_depend_sql =
		"insert or ignore into filter(oid, restore_list_name, kind) "
		"     select distinct objid, identity as restore_list_name, 'pg_depend' "
		"       from s_depend d "
		"      where not exists"
		"            (select 1 from source.s_seq ss where ss.oid = d.objid) ";

	if (!catalog_sql_prepare(db, s_depend_sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which does not return any row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * Implement --skip-extensions
	 */
	if (skipExtensions)
	{
		char *s_extension_sql =
			"insert or ignore into filter(oid, restore_list_name, kind) "
			"     select oid, extname, 'extension' "
			"       from s_extension ";

		if (!catalog_sql_prepare(db, s_extension_sql, &query))
		{
			/* errors have already been logged */
			return false;
		}

		/* now execute the query, which does not return any row */
		if (!catalog_sql_execute_once(&query))
		{
			/* errors have already been logged */
			return false;
		}
	}

	/*
	 * Implement --skip-collations
	 */
	if (skipCollations)
	{
		char *s_coll_sql =
			"insert or ignore into filter(oid, restore_list_name, kind) "
			"    select oid, restore_list_name, 'coll' "
			"      from s_coll ";

		if (!catalog_sql_prepare(db, s_coll_sql, &query))
		{
			/* errors have already been logged */
			return false;
		}

		/* now execute the query, which does not return any row */
		if (!catalog_sql_execute_once(&query))
		{
			/* errors have already been logged */
			return false;
		}
	}

	return true;
}


/*
 * catalog_lookup_filter_by_oid fetches a  entry from our catalogs.
 */
bool
catalog_lookup_filter_by_oid(DatabaseCatalog *catalog,
							 CatalogFilter *result,
							 uint32_t oid)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_lookup_filter_by_oid: db is NULL");
		return false;
	}

	char *sql =
		"  select oid, restore_list_name, kind "
		"    from filter "
		"   where oid = $1 ";

	SQLiteQuery query = {
		.context = result,
		.fetchFunction = &catalog_filter_fetch
	};

	if (!semaphore_lock(&(catalog->sema)))
	{
		/* errors have already been logged */
		return false;
	}

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* bind our parameters now */
	BindParam params[1] = {
		{ BIND_PARAMETER_TYPE_INT64, "oid", oid, NULL },
	};

	if (!catalog_sql_bind(&query, params, 1))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* now execute the query, which return exactly one row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	(void) semaphore_unlock(&(catalog->sema));

	return true;
}


/*
 * catalog_lookup_filter_by_rlname fetches a filter entry from our catalogs.
 */
bool
catalog_lookup_filter_by_rlname(DatabaseCatalog *catalog,
								CatalogFilter *result,
								const char *restoreListName)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_lookup_filter_by_oid: db is NULL");
		return false;
	}

	/*
	 * In the case of archive entries for SEQUENCE, SEQUENCE OWNED BY, and
	 * DEFAULT values that depend on sequences, we might find the same sequence
	 * restore_list_name more than once with different values for the OID (the
	 * sequence oid, NUL, or the attroid oid).
	 *
	 * Because of that, add a LIMIT 1 to our query here to avoid throwing an
	 * SQLite error condition about "another row available".
	 */
	char *sql =
		"  select oid, restore_list_name, kind "
		"    from filter "
		"   where restore_list_name = $1 "
		"   limit 1";

	SQLiteQuery query = {
		.context = result,
		.fetchFunction = &catalog_filter_fetch
	};

	if (!semaphore_lock(&(catalog->sema)))
	{
		/* errors have already been logged */
		return false;
	}

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* bind our parameters now */
	BindParam params[1] = {
		{ BIND_PARAMETER_TYPE_TEXT, "restore_list_name", 0,
		  (char *) restoreListName },
	};

	if (!catalog_sql_bind(&query, params, 1))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* now execute the query, which return exactly one row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	(void) semaphore_unlock(&(catalog->sema));

	return true;
}


/*
 * catalog_filter_fetch fetches a CatalogFilter entry from a SQLite ppStmt
 * result set.
 */
bool
catalog_filter_fetch(SQLiteQuery *query)
{
	CatalogFilter *entry = (CatalogFilter *) query->context;

	/* cleanup the memory area before re-use */
	bzero(entry, sizeof(CatalogFilter));

	entry->oid = sqlite3_column_int64(query->ppStmt, 0);

	if (sqlite3_column_type(query->ppStmt, 1) != SQLITE_NULL)
	{
		strlcpy(entry->restoreListName,
				(char *) sqlite3_column_text(query->ppStmt, 1),
				sizeof(entry->restoreListName));
	}

	strlcpy(entry->kind,
			(char *) sqlite3_column_text(query->ppStmt, 2),
			sizeof(entry->kind));

	return true;
}


/*
 * catalog_add_s_database INSERTs a SourceDatabase to our internal catalogs
 * database.
 */
bool
catalog_add_s_database(DatabaseCatalog *catalog, SourceDatabase *dat)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_add_s_database: db is NULL");
		return false;
	}

	char *sql =
		"insert or replace into s_database(oid, datname, bytes, bytes_pretty)"
		"values($1, $2, $3, $4)";

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "oid", dat->oid, NULL },
		{ BIND_PARAMETER_TYPE_TEXT, "datname", 0, dat->datname },
		{ BIND_PARAMETER_TYPE_INT64, "bytes", dat->bytes, NULL },
		{ BIND_PARAMETER_TYPE_TEXT, "bytes_pretty", 0, dat->bytesPretty }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which does not return any row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_add_s_database_properties INSERTs a SourceProperty to our internal
 * catalogs database.
 */
bool
catalog_add_s_database_properties(DatabaseCatalog *catalog, SourceProperty *guc)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_add_s_database_properties: db is NULL");
		return false;
	}

	char *sql =
		"insert or replace into s_database_property("
		"  role_in_database, rolname, datname, setconfig)"
		"values($1, $2, $3, $4)";

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT, "role_in_database",
		  guc->roleInDatabase ? 1 : 0, NULL },

		{ BIND_PARAMETER_TYPE_TEXT, "rolname", 0, guc->rolname },
		{ BIND_PARAMETER_TYPE_TEXT, "datname", 0, guc->datname },
		{ BIND_PARAMETER_TYPE_TEXT, "setconfig", 0, guc->setconfig }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which does not return any row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_iter_s_database iterates over the list of databases in our catalogs.
 */
bool
catalog_iter_s_database(DatabaseCatalog *catalog,
						void *context,
						SourceDatabaseIterFun *callback)
{
	SourceDatabaseIterator *iter =
		(SourceDatabaseIterator *) calloc(1, sizeof(SourceDatabaseIterator));

	iter->catalog = catalog;

	if (!catalog_iter_s_database_init(iter))
	{
		/* errors have already been logged */
		return false;
	}

	for (;;)
	{
		if (!catalog_iter_s_database_next(iter))
		{
			/* errors have already been logged */
			return false;
		}

		SourceDatabase *dat = iter->dat;

		if (dat == NULL)
		{
			if (!catalog_iter_s_database_finish(iter))
			{
				/* errors have already been logged */
				return false;
			}

			break;
		}

		/* now call the provided callback */
		if (!(*callback)(context, dat))
		{
			log_error("Failed to iterate over list of dats, "
					  "see above for details");
			return false;
		}
	}


	return true;
}


/*
 * catalog_iter_s_database_init initializes an Interator over our catalog of
 * SourceDatabase entries.
 */
bool
catalog_iter_s_database_init(SourceDatabaseIterator *iter)
{
	sqlite3 *db = iter->catalog->db;

	if (db == NULL)
	{
		log_error("BUG: Failed to initialize s_dat iterator: db is NULL");
		return false;
	}

	iter->dat = (SourceDatabase *) calloc(1, sizeof(SourceDatabase));

	if (iter->dat == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	char *sql =
		"  select oid, datname, bytes, bytes_pretty"
		"    from s_database "
		"order by datname";

	SQLiteQuery *query = &(iter->query);

	query->context = iter->dat;
	query->fetchFunction = &catalog_s_database_fetch;

	if (!catalog_sql_prepare(db, sql, query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_iter_s_database_next fetches the next SourceDatabase entry in our
 * catalogs.
 */
bool
catalog_iter_s_database_next(SourceDatabaseIterator *iter)
{
	SQLiteQuery *query = &(iter->query);

	int rc = catalog_sql_step(query);

	if (rc == SQLITE_DONE)
	{
		iter->dat = NULL;

		return true;
	}

	if (rc != SQLITE_ROW)
	{
		log_error("Failed to step through statement: %s", query->sql);
		log_error("[SQLite] %s", sqlite3_errmsg(query->db));
		return false;
	}

	return catalog_s_database_fetch(query);
}


/*
 * catalog_s_dat_fetch fetches a SourceDatabase entry from a SQLite ppStmt
 * result set.
 */
bool
catalog_s_database_fetch(SQLiteQuery *query)
{
	SourceDatabase *dat = (SourceDatabase *) query->context;

	/* cleanup the memory area before re-use */
	bzero(dat, sizeof(SourceDatabase));

	dat->oid = sqlite3_column_int64(query->ppStmt, 0);

	strlcpy(dat->datname,
			(char *) sqlite3_column_text(query->ppStmt, 1),
			sizeof(dat->datname));

	dat->bytes = sqlite3_column_int64(query->ppStmt, 2);

	strlcpy(dat->bytesPretty,
			(char *) sqlite3_column_text(query->ppStmt, 3),
			sizeof(dat->bytesPretty));

	return true;
}


/*
 * catalog_iter_s_database_finish cleans-up the internal memory used for the
 * iteration.
 */
bool
catalog_iter_s_database_finish(SourceDatabaseIterator *iter)
{
	SQLiteQuery *query = &(iter->query);

	/* in case we finish before reaching the DONE step */
	if (iter->dat != NULL)
	{
		iter->dat = NULL;
	}

	if (!catalog_sql_finalize(query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_iter_s_database_guc iterates over the list of database properties in
 * our catalogs.
 */
bool
catalog_iter_s_database_guc(DatabaseCatalog *catalog,
							const char *dbname,
							void *context,
							SourcePropertyIterFun *callback)
{
	SourcePropertyIterator *iter =
		(SourcePropertyIterator *) calloc(1, sizeof(SourcePropertyIterator));

	iter->catalog = catalog;
	iter->dbname = dbname;

	if (!catalog_iter_s_database_guc_init(iter))
	{
		/* errors have already been logged */
		return false;
	}

	for (;;)
	{
		if (!catalog_iter_s_database_guc_next(iter))
		{
			/* errors have already been logged */
			return false;
		}

		SourceProperty *property = iter->property;

		if (property == NULL)
		{
			if (!catalog_iter_s_database_guc_finish(iter))
			{
				/* errors have already been logged */
				return false;
			}

			break;
		}

		/* now call the provided callback */
		if (!(*callback)(context, property))
		{
			log_error("Failed to iterate over list of dats, "
					  "see above for details");
			return false;
		}
	}


	return true;
}


/*
 * catalog_iter_s_database_guc_init initializes an Interator over our catalog of
 * SourceProperty entries.
 */
bool
catalog_iter_s_database_guc_init(SourcePropertyIterator *iter)
{
	sqlite3 *db = iter->catalog->db;

	if (db == NULL)
	{
		log_error("BUG: Failed to initialize s_database_guc iterator: db is NULL");
		return false;
	}

	iter->property = (SourceProperty *) calloc(1, sizeof(SourceProperty));

	if (iter->property == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	char *sql =
		"  select role_in_database, rolname, datname, setconfig"
		"    from s_database_property "
		"   where datname = $1 ";

	SQLiteQuery *query = &(iter->query);

	query->context = iter->property;
	query->fetchFunction = &catalog_s_database_guc_fetch;

	if (!catalog_sql_prepare(db, sql, query))
	{
		/* errors have already been logged */
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_TEXT, "datname", 0, (char *) iter->dbname }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(query, params, count))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_iter_s_database_guc_next fetches the next SourceProperty entry in our
 * catalogs.
 */
bool
catalog_iter_s_database_guc_next(SourcePropertyIterator *iter)
{
	SQLiteQuery *query = &(iter->query);

	int rc = catalog_sql_step(query);

	if (rc == SQLITE_DONE)
	{
		iter->property = NULL;

		return true;
	}

	if (rc != SQLITE_ROW)
	{
		log_error("Failed to step through statement: %s", query->sql);
		log_error("[SQLite] %s", sqlite3_errmsg(query->db));
		return false;
	}

	return catalog_s_database_guc_fetch(query);
}


/*
 * catalog_s_dat_fetch fetches a SourceProperty entry from a SQLite ppStmt
 * result set.
 */
bool
catalog_s_database_guc_fetch(SQLiteQuery *query)
{
	SourceProperty *property = (SourceProperty *) query->context;

	/* cleanup the memory area before re-use */
	bzero(property, sizeof(SourceProperty));

	property->roleInDatabase = sqlite3_column_int(query->ppStmt, 0) == 1;

	if (sqlite3_column_type(query->ppStmt, 1) != SQLITE_NULL)
	{
		strlcpy(property->rolname,
				(char *) sqlite3_column_text(query->ppStmt, 1),
				sizeof(property->rolname));
	}

	if (sqlite3_column_type(query->ppStmt, 2) != SQLITE_NULL)
	{
		strlcpy(property->datname,
				(char *) sqlite3_column_text(query->ppStmt, 2),
				sizeof(property->datname));
	}

	if (sqlite3_column_type(query->ppStmt, 3) != SQLITE_NULL)
	{
		int len = sqlite3_column_bytes(query->ppStmt, 3);
		int bytes = len + 1;

		property->setconfig = (char *) calloc(bytes, sizeof(char));

		if (property->setconfig == NULL)
		{
			log_fatal(ALLOCATION_FAILED_ERROR);
			return false;
		}

		strlcpy(property->setconfig,
				(char *) sqlite3_column_text(query->ppStmt, 3),
				bytes);
	}

	return true;
}


/*
 * catalog_iter_s_database_guc_finish cleans-up the internal memory used for the
 * iteration.
 */
bool
catalog_iter_s_database_guc_finish(SourcePropertyIterator *iter)
{
	SQLiteQuery *query = &(iter->query);

	/* in case we finish before reaching the DONE step */
	if (iter->property != NULL)
	{
		iter->property = NULL;
	}

	if (!catalog_sql_finalize(query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_add_s_coll INSERTs a SourceSchema to our internal catalogs
 * database.
 */
bool
catalog_add_s_coll(DatabaseCatalog *catalog, SourceCollation *coll)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_add_s_coll: db is NULL");
		return false;
	}

	char *sql =
		"insert into s_coll(oid, collname, description, restore_list_name) "
		"values($1, $2, $3, $4)";

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "oid", coll->oid, NULL },
		{ BIND_PARAMETER_TYPE_TEXT, "nspname", 0, coll->collname },
		{ BIND_PARAMETER_TYPE_TEXT, "description", 0, coll->desc },

		{ BIND_PARAMETER_TYPE_TEXT, "restore_list_name", 0,
		  coll->restoreListName }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which does not return any row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_iter_s_coll iterates over the list of datuences in our catalogs.
 */
bool
catalog_iter_s_coll(DatabaseCatalog *catalog,
					void *context,
					SourceCollationIterFun *callback)
{
	SourceCollationIterator *iter =
		(SourceCollationIterator *) calloc(1, sizeof(SourceCollationIterator));

	iter->catalog = catalog;

	if (!catalog_iter_s_coll_init(iter))
	{
		/* errors have already been logged */
		return false;
	}

	for (;;)
	{
		if (!catalog_iter_s_coll_next(iter))
		{
			/* errors have already been logged */
			return false;
		}

		SourceCollation *coll = iter->coll;

		if (coll == NULL)
		{
			if (!catalog_iter_s_coll_finish(iter))
			{
				/* errors have already been logged */
				return false;
			}

			break;
		}

		/* now call the provided callback */
		if (!(*callback)(context, coll))
		{
			log_error("Failed to iterate over list of colls, "
					  "see above for details");
			return false;
		}
	}


	return true;
}


/*
 * catalog_iter_s_coll_init initializes an Interator over our catalog of
 * SourceCollation entries.
 */
bool
catalog_iter_s_coll_init(SourceCollationIterator *iter)
{
	sqlite3 *db = iter->catalog->db;

	if (db == NULL)
	{
		log_error("BUG: Failed to initialize s_coll iterator: db is NULL");
		return false;
	}

	iter->coll = (SourceCollation *) calloc(1, sizeof(SourceCollation));

	if (iter->coll == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	char *sql =
		"  select oid, collname, description, restore_list_name"
		"    from s_coll "
		"order by oid";

	SQLiteQuery *query = &(iter->query);

	query->context = iter->coll;
	query->fetchFunction = &catalog_s_coll_fetch;

	if (!catalog_sql_prepare(db, sql, query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_iter_s_coll_next fetches the next SourceCollation entry in our
 * catalogs.
 */
bool
catalog_iter_s_coll_next(SourceCollationIterator *iter)
{
	SQLiteQuery *query = &(iter->query);

	int rc = catalog_sql_step(query);

	if (rc == SQLITE_DONE)
	{
		iter->coll = NULL;

		return true;
	}

	if (rc != SQLITE_ROW)
	{
		log_error("Failed to step through statement: %s", query->sql);
		log_error("[SQLite] %s", sqlite3_errmsg(query->db));
		return false;
	}

	return catalog_s_coll_fetch(query);
}


/*
 * catalog_s_coll_fetch fetches a SourceCollation entry from a SQLite ppStmt
 * result set.
 */
bool
catalog_s_coll_fetch(SQLiteQuery *query)
{
	SourceCollation *coll = (SourceCollation *) query->context;

	/* cleanup the memory area before re-use */
	bzero(coll, sizeof(SourceCollation));

	coll->oid = sqlite3_column_int64(query->ppStmt, 0);

	strlcpy(coll->collname,
			(char *) sqlite3_column_text(query->ppStmt, 1),
			sizeof(coll->collname));

	/* coll->desc is a malloc'ed area */
	if (sqlite3_column_type(query->ppStmt, 2) != SQLITE_NULL)
	{
		int len = sqlite3_column_bytes(query->ppStmt, 2);
		int bytes = len + 1;

		coll->desc = (char *) calloc(bytes, sizeof(char));

		if (coll->desc == NULL)
		{
			log_fatal(ALLOCATION_FAILED_ERROR);
			return false;
		}

		strlcpy(coll->desc,
				(char *) sqlite3_column_text(query->ppStmt, 2),
				bytes);
	}

	strlcpy(coll->restoreListName,
			(char *) sqlite3_column_text(query->ppStmt, 3),
			sizeof(coll->restoreListName));

	return true;
}


/*
 * catalog_iter_s_coll_finish cleans-up the internal memory used for the
 * iteration.
 */
bool
catalog_iter_s_coll_finish(SourceCollationIterator *iter)
{
	SQLiteQuery *query = &(iter->query);

	/* in case we finish before reaching the DONE step */
	if (iter->coll != NULL)
	{
		iter->coll = NULL;
	}

	if (!catalog_sql_finalize(query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_add_s_namespace INSERTs a SourceSchema to our internal catalogs
 * database.
 */
bool
catalog_add_s_namespace(DatabaseCatalog *catalog, SourceSchema *namespace)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_add_s_namespace: db is NULL");
		return false;
	}

	char *sql =
		"insert into s_namespace(oid, nspname, restore_list_name) "
		"values($1, $2, $3)";

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "oid", namespace->oid, NULL },
		{ BIND_PARAMETER_TYPE_TEXT, "nspname", 0, namespace->nspname },

		{ BIND_PARAMETER_TYPE_TEXT, "restore_list_name", 0,
		  namespace->restoreListName }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which does not return any row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_lookup_s_namespace_by_nspname fetches a s_namespace entry from our
 * catalogs.
 */
bool
catalog_lookup_s_namespace_by_nspname(DatabaseCatalog *catalog,
									  const char *nspname,
									  SourceSchema *result)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_lookup_s_namespace_by_nspname: db is NULL");
		return false;
	}

	char *sql =
		"  select oid, nspname, restore_list_name "
		"    from s_namespace "
		"   where nspname = $1 ";

	SQLiteQuery query = {
		.context = result,
		.fetchFunction = &catalog_s_namespace_fetch
	};

	if (!semaphore_lock(&(catalog->sema)))
	{
		/* errors have already been logged */
		return false;
	}

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* bind our parameters now */
	BindParam params[1] = {
		{ BIND_PARAMETER_TYPE_TEXT, "nspname", 0,
		  (char *) nspname },
	};

	if (!catalog_sql_bind(&query, params, 1))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* now execute the query, which return exactly one row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	(void) semaphore_unlock(&(catalog->sema));

	return true;
}


/*
 * catalog_lookup_s_namespace_by_oid fetches a s_namespace entry from our
 * catalogs using the oid.
 */
bool
catalog_lookup_s_namespace_by_oid(DatabaseCatalog *catalog,
								  uint32_t oid,
								  SourceSchema *result)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_lookup_s_namespace_by_oid: db is NULL");
		return false;
	}

	char *sql =
		"  select oid, nspname, restore_list_name "
		"    from s_namespace "
		"   where oid = $1 ";

	SQLiteQuery query = {
		.context = result,
		.fetchFunction = &catalog_s_namespace_fetch
	};

	if (!semaphore_lock(&(catalog->sema)))
	{
		/* errors have already been logged */
		return false;
	}

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* bind our parameters now */
	BindParam params[1] = {
		{ BIND_PARAMETER_TYPE_INT64, "oid", oid, NULL },
	};

	if (!catalog_sql_bind(&query, params, 1))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* now execute the query, which return exactly one row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	(void) semaphore_unlock(&(catalog->sema));

	return true;
}


/*
 * catalog_s_extension_fetch fetches a SourceExtension entry from a SQLite
 * ppStmt result set.
 */
bool
catalog_s_namespace_fetch(SQLiteQuery *query)
{
	SourceSchema *schema = (SourceSchema *) query->context;

	/* cleanup the memory area before re-use */
	bzero(schema, sizeof(SourceSchema));

	schema->oid = sqlite3_column_int64(query->ppStmt, 0);

	strlcpy(schema->nspname,
			(char *) sqlite3_column_text(query->ppStmt, 1),
			sizeof(schema->nspname));

	strlcpy(schema->restoreListName,
			(char *) sqlite3_column_text(query->ppStmt, 2),
			sizeof(schema->restoreListName));

	return true;
}


/*
 * catalog_add_s_extension INSERTs a SourceExtension to our internal catalogs
 * database.
 */
bool
catalog_add_s_extension(DatabaseCatalog *catalog, SourceExtension *extension)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_add_s_extension: db is NULL");
		return false;
	}

	char *sql =
		"insert into s_extension(oid, extname, extnamespace, extrelocatable) "
		"values($1, $2, $3, $4)";

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "oid", extension->oid, NULL },
		{ BIND_PARAMETER_TYPE_TEXT, "extname", 0, extension->extname },
		{ BIND_PARAMETER_TYPE_TEXT, "extnamespace", 0, extension->extnamespace },

		{ BIND_PARAMETER_TYPE_INT, "extrelocatable",
		  extension->extrelocatable ? 1 : 0, NULL }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which does not return any row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_add_s_extension_config INSERTs a SourceExtensionConfig to our internal
 * catalogs database.
 */
bool
catalog_add_s_extension_config(DatabaseCatalog *catalog,
							   SourceExtensionConfig *config)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_add_s_extension_config: db is NULL");
		return false;
	}

	char *sql =
		"insert into s_extension_config"
		"  (extoid, reloid, nspname, relname, condition, relkind) "
		"values($1, $2, $3, $4, $5, $6)";

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "extoid", config->extoid, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "reloid", config->reloid, NULL },
		{ BIND_PARAMETER_TYPE_TEXT, "nspname", 0, config->nspname },
		{ BIND_PARAMETER_TYPE_TEXT, "relname", 0, config->relname },
		{ BIND_PARAMETER_TYPE_TEXT, "condition", 0, config->condition },
		{ BIND_PARAMETER_TYPE_INT, "relkind", (int) config->relkind, NULL }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which does not return any row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/* 
*   catalog_iter_s_extesnion_timescaledb_checker , iterates over the list of extensions in our catalogs 
*   and checks for presence of timescaledb extension.
*/
bool
catalog_iter_s_extension_timescaledb_checker(DatabaseCatalog *catalog,
											 bool *timescaledb)
{
	SourceExtensionIterator *iter =
		(SourceExtensionIterator *) calloc(1, sizeof(SourceExtensionIterator));

	iter->catalog = catalog;

	*timescaledb=false;

	if (!catalog_iter_s_extension_init(iter))
	{
		/* errors have already been logged */
		return false;
	}

	for (;;)
	{

		if (!catalog_iter_s_extension_next(iter))
		{
			/* errors have already been logged */
			return false;
		}

		SourceExtension *ext = iter->ext;

		if (ext == NULL)
		{
			if (!catalog_iter_s_extension_finish(iter))
			{
				/* errors have already been logged */
				return false;
			}

			break;
		}

		if(strcmp(ext->extname, "timescaledb") == 0)
		{
			if(!catalog_iter_s_extension_finish(iter))
			{
				return false;
			}

			*timescaledb = true;
			
			return true;

		}

	}


	return true;
}

/*
 * catalog_iter_s_extension iterates over the list of extensions in our
 * catalogs.
 */
bool
catalog_iter_s_extension(DatabaseCatalog *catalog,
						 void *context,
						 SourceExtensionIterFun *callback)
{
	SourceExtensionIterator *iter =
		(SourceExtensionIterator *) calloc(1, sizeof(SourceExtensionIterator));

	iter->catalog = catalog;

	if (!catalog_iter_s_extension_init(iter))
	{
		/* errors have already been logged */
		return false;
	}

	for (;;)
	{
		if (!catalog_iter_s_extension_next(iter))
		{
			/* errors have already been logged */
			return false;
		}

		SourceExtension *ext = iter->ext;

		if (ext == NULL)
		{
			if (!catalog_iter_s_extension_finish(iter))
			{
				/* errors have already been logged */
				return false;
			}

			break;
		}

		/* now call the provided callback */
		if (!(*callback)(context, ext))
		{
			log_error("Failed to iterate over list of extensions, "
					  "see above for details");
			return false;
		}
	}


	return true;
}


/*
 * catalog_iter_s_extension_init initializes an Interator over our catalog of
 * SourceExtension entries.
 */
bool
catalog_iter_s_extension_init(SourceExtensionIterator *iter)
{
	sqlite3 *db = iter->catalog->db;

	if (db == NULL)
	{
		log_error("BUG: Failed to initialize s_extension iterator: db is NULL");
		return false;
	}

	iter->ext = (SourceExtension *) calloc(1, sizeof(SourceExtension));

	if (iter->ext == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	char *sql =
		"  select oid, extname, extnamespace, extrelocatable "
		"    from s_extension "
		"order by extname";

	SQLiteQuery *query = &(iter->query);

	query->context = iter->ext;
	query->fetchFunction = &catalog_s_extension_fetch;

	if (!catalog_sql_prepare(db, sql, query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_iter_s_extension_next fetches the next SourceExtension entry in our
 * catalogs.
 */
bool
catalog_iter_s_extension_next(SourceExtensionIterator *iter)
{
	SQLiteQuery *query = &(iter->query);

	int rc = catalog_sql_step(query);

	if (rc == SQLITE_DONE)
	{
		iter->ext = NULL;

		return true;
	}

	if (rc != SQLITE_ROW)
	{
		log_error("Failed to step through statement: %s", query->sql);
		log_error("[SQLite] %s", sqlite3_errmsg(query->db));
		return false;
	}

	return catalog_s_extension_fetch(query);
}


/*
 * catalog_s_extension_fetch fetches a SourceExtension entry from a SQLite
 * ppStmt result set.
 */
bool
catalog_s_extension_fetch(SQLiteQuery *query)
{
	SourceExtension *ext = (SourceExtension *) query->context;

	/* cleanup the memory area before re-use */
	bzero(ext, sizeof(SourceExtension));

	ext->oid = sqlite3_column_int64(query->ppStmt, 0);

	strlcpy(ext->extname,
			(char *) sqlite3_column_text(query->ppStmt, 1),
			sizeof(ext->extname));

	strlcpy(ext->extnamespace,
			(char *) sqlite3_column_text(query->ppStmt, 2),
			sizeof(ext->extnamespace));

	ext->extrelocatable = sqlite3_column_int(query->ppStmt, 3) == 1;

	return true;
}


/*
 * catalog_iter_s_extension_finish cleans-up the internal memory used for the
 * iteration.
 */
bool
catalog_iter_s_extension_finish(SourceExtensionIterator *iter)
{
	SQLiteQuery *query = &(iter->query);

	/* in case we finish before reaching the DONE step */
	if (iter->ext != NULL)
	{
		iter->ext = NULL;
	}

	if (!catalog_sql_finalize(query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_s_ext_fetch_extconfig fetches the ext SourceExtensionConfig array
 * from our s_extension_config catalog.
 */
bool
catalog_s_ext_fetch_extconfig(DatabaseCatalog *catalog, SourceExtension *ext)
{
	SourceExtConfigIterator *iter =
		(SourceExtConfigIterator *) calloc(1, sizeof(SourceExtConfigIterator));

	if (iter == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	iter->catalog = catalog;
	iter->ext = ext;

	if (!catalog_iter_s_ext_extconfig_init(iter))
	{
		/* errors have already been logged */
		return false;
	}

	while (!iter->done)
	{
		if (!catalog_iter_s_ext_extconfig_next(iter))
		{
			/* errors have already been logged */
			return false;
		}
	}

	if (!catalog_iter_s_ext_extconfig_finish(iter))
	{
		/* errors have already been logged */
		return false;
	}


	return true;
}


/*
 * catalog_iter_s_ext_extconfig_init initializes an Interator over our catalog
 * of SourceExtensionConfig entries.
 */
bool
catalog_iter_s_ext_extconfig_init(SourceExtConfigIterator *iter)
{
	sqlite3 *db = iter->catalog->db;

	if (db == NULL)
	{
		log_error("BUG: Failed to initialize s_ext iterator: db is NULL");
		return false;
	}

	SourceExtension *ext = iter->ext;

	if (iter->ext == NULL)
	{
		log_error("BUG: Failed to initialize s_ext iterator: ext is NULL");
		return false;
	}

	/*
	 * Query extension config table based on the order at which it is
	 * inserted using sqlite's inbuilt "rowid". The insertion order ensures
	 * that the config tables are inserted according to it's foreign key
	 * dependency.
	 */
	char *sql =
		"  select count(*) over(order by rowid) as num,  "
		"         count(*) over() as count, "
		"         oid, reloid, nspname, relname, condition, relkind "
		"    from s_extension_config "
		"   where extoid = $1 "
		"order by rowid";

	SQLiteQuery *query = &(iter->query);

	query->context = iter->ext;
	query->fetchFunction = &catalog_s_ext_extconfig_fetch;

	if (!catalog_sql_prepare(db, sql, query))
	{
		/* errors have already been logged */
		return false;
	}

	/* bind our parameters now */
	BindParam params[1] = {
		{ BIND_PARAMETER_TYPE_INT64, "oid", ext->oid, NULL }
	};

	if (!catalog_sql_bind(query, params, 1))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_iter_s_ext_extconfig_next fetches the next SourceExtensionConfig
 * entry in our catalogs.
 */
bool
catalog_iter_s_ext_extconfig_next(SourceExtConfigIterator *iter)
{
	SQLiteQuery *query = &(iter->query);

	int rc = catalog_sql_step(query);

	if (rc == SQLITE_DONE)
	{
		iter->done = true;
		return true;
	}

	if (rc != SQLITE_ROW)
	{
		log_error("Failed to step through statement: %s", query->sql);
		log_error("[SQLite] %s", sqlite3_errmsg(query->db));
		return false;
	}

	return catalog_s_ext_extconfig_fetch(query);
}


/*
 * catalog_iter_s_ext_extconfig_finish cleans-up the internal memory used for the
 * iteration.
 */
bool
catalog_iter_s_ext_extconfig_finish(SourceExtConfigIterator *iter)
{
	SQLiteQuery *query = &(iter->query);

	if (!catalog_sql_finalize(query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_s_ext_extconfig_fetch is a SQLiteQuery callback.
 */
bool
catalog_s_ext_extconfig_fetch(SQLiteQuery *query)
{
	SourceExtension *ext = (SourceExtension *) query->context;

	int num = sqlite3_column_int(query->ppStmt, 0);
	int count = sqlite3_column_int(query->ppStmt, 1);

	if (num == 1)
	{
		ext->config.count = count;
		ext->config.array =
			(SourceExtensionConfig *) calloc(count,
											 sizeof(SourceExtensionConfig));

		if (ext->config.array == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}
	}

	SourceExtensionConfig *conf = &(ext->config.array[num - 1]);

	conf->extoid = sqlite3_column_int64(query->ppStmt, 2);
	conf->reloid = sqlite3_column_int64(query->ppStmt, 3);

	strlcpy(conf->nspname,
			(char *) sqlite3_column_text(query->ppStmt, 4),
			sizeof(conf->nspname));

	strlcpy(conf->relname,
			(char *) sqlite3_column_text(query->ppStmt, 5),
			sizeof(conf->relname));

	/* config->condition is a malloc'ed area */
	if (sqlite3_column_type(query->ppStmt, 6) != SQLITE_NULL)
	{
		int len = sqlite3_column_bytes(query->ppStmt, 6);
		int bytes = len + 1;

		conf->condition = (char *) calloc(bytes, sizeof(char));

		if (conf->condition == NULL)
		{
			log_fatal(ALLOCATION_FAILED_ERROR);
			return false;
		}

		strlcpy(conf->condition,
				(char *) sqlite3_column_text(query->ppStmt, 6),
				bytes);
	}

	conf->relkind = sqlite3_column_int(query->ppStmt, 7);

	return true;
}


/*
 * catalog_add_s_role INSERTs a SourceRole to our internal catalogs
 * database.
 */
bool
catalog_add_s_role(DatabaseCatalog *catalog, SourceRole *role)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_add_s_role: db is NULL");
		return false;
	}

	char *sql =
		"insert into s_role(oid, rolname) values($1, $2)";

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "oid", role->oid, NULL },
		{ BIND_PARAMETER_TYPE_TEXT, "rolname", 0, role->rolname }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which does not return any row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_lookup_s_role fetches a SourceRole entry from our catalogs.
 */
bool
catalog_lookup_s_role_by_name(DatabaseCatalog *catalog,
							  const char *rolname,
							  SourceRole *role)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_lookup_s_role_by_name: db is NULL");
		return false;
	}

	SQLiteQuery query = {
		.context = role,
		.fetchFunction = &catalog_s_role_fetch
	};

	char *sql =
		"  select oid, rolname "
		"    from s_role"
		"   where rolname = $1 ";

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_TEXT, "rolname", 0, (char *) rolname }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which return exactly one row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_s_role_stats_fetch is a SQLiteQuery callback.
 */
bool
catalog_s_role_fetch(SQLiteQuery *query)
{
	SourceRole *role = (SourceRole *) query->context;

	role->oid = sqlite3_column_int64(query->ppStmt, 0);

	strlcpy(role->rolname,
			(char *) sqlite3_column_text(query->ppStmt, 1),
			sizeof(role->rolname));

	return true;
}


/*
 * catalog_add_s_depend INSERTs a SourceDepend to our internal catalogs
 * database.
 */
bool
catalog_add_s_depend(DatabaseCatalog *catalog, SourceDepend *depend)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_add_s_depend: db is NULL");
		return false;
	}

	char *sql =
		"insert into s_depend("
		"  nspname, relname, refclassid, refobjid, classid, objid, "
		"  deptype, type, identity)"
		"values($1, $2, $3, $4, $5, $6, $7, $8, $9)";

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* depend->deptype is a single char, we want a C-string */
	char deptype[2] = " ";
	deptype[0] = depend->deptype;

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_TEXT, "nspname", 0, depend->nspname },
		{ BIND_PARAMETER_TYPE_TEXT, "relname", 0, depend->relname },
		{ BIND_PARAMETER_TYPE_INT64, "refclassid", depend->refclassid, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "refobjid", depend->refobjid, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "classid", depend->classid, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "objid", depend->objid, NULL },
		{ BIND_PARAMETER_TYPE_TEXT, "deptype", 0, deptype },
		{ BIND_PARAMETER_TYPE_TEXT, "type", 0, depend->type },
		{ BIND_PARAMETER_TYPE_TEXT, "identity", 0, depend->identity }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which does not return any row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_iter_s_depend iterates over the list of datuences in our catalogs.
 */
bool
catalog_iter_s_depend(DatabaseCatalog *catalog,
					  void *context,
					  SourceDependIterFun *callback)
{
	SourceDependIterator *iter =
		(SourceDependIterator *) calloc(1, sizeof(SourceDependIterator));

	iter->catalog = catalog;

	if (!catalog_iter_s_depend_init(iter))
	{
		/* errors have already been logged */
		return false;
	}

	for (;;)
	{
		if (!catalog_iter_s_depend_next(iter))
		{
			/* errors have already been logged */
			return false;
		}

		SourceDepend *dep = iter->dep;

		if (dep == NULL)
		{
			if (!catalog_iter_s_depend_finish(iter))
			{
				/* errors have already been logged */
				return false;
			}

			break;
		}

		/* now call the provided callback */
		if (!(*callback)(context, dep))
		{
			log_error("Failed to iterate over list of deps, "
					  "see above for details");
			return false;
		}
	}


	return true;
}


/*
 * catalog_iter_s_depend_init initializes an Interator over our catalog of
 * SourceDepend entries.
 */
bool
catalog_iter_s_depend_init(SourceDependIterator *iter)
{
	sqlite3 *db = iter->catalog->db;

	if (db == NULL)
	{
		log_error("BUG: Failed to initialize s_depend iterator: db is NULL");
		return false;
	}

	iter->dep = (SourceDepend *) calloc(1, sizeof(SourceDepend));

	if (iter->dep == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	char *sql =
		"  select nspname, relname, refclassid, refobjid, classid, objid, "
		"         deptype, type, identity "
		"    from s_depend "
		"order by nspname, relname, refclassid";

	SQLiteQuery *query = &(iter->query);

	query->context = iter->dep;
	query->fetchFunction = &catalog_s_depend_fetch;

	if (!catalog_sql_prepare(db, sql, query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_iter_s_depend_next fetches the next SourceDepend entry in our
 * catalogs.
 */
bool
catalog_iter_s_depend_next(SourceDependIterator *iter)
{
	SQLiteQuery *query = &(iter->query);

	int rc = catalog_sql_step(query);

	if (rc == SQLITE_DONE)
	{
		iter->dep = NULL;

		return true;
	}

	if (rc != SQLITE_ROW)
	{
		log_error("Failed to step through statement: %s", query->sql);
		log_error("[SQLite] %s", sqlite3_errmsg(query->db));
		return false;
	}

	return catalog_s_depend_fetch(query);
}


/*
 * catalog_s_depend_fetch fetches a SourceDepend entry from a SQLite ppStmt
 * result set.
 */
bool
catalog_s_depend_fetch(SQLiteQuery *query)
{
	SourceDepend *dep = (SourceDepend *) query->context;

	/* cleanup the memory area before re-use */
	bzero(dep, sizeof(SourceDepend));

	strlcpy(dep->nspname,
			(char *) sqlite3_column_text(query->ppStmt, 0),
			sizeof(dep->nspname));

	strlcpy(dep->relname,
			(char *) sqlite3_column_text(query->ppStmt, 1),
			sizeof(dep->relname));

	dep->refclassid = sqlite3_column_int64(query->ppStmt, 2);
	dep->refobjid = sqlite3_column_int64(query->ppStmt, 3);
	dep->classid = sqlite3_column_int64(query->ppStmt, 4);
	dep->objid = sqlite3_column_int64(query->ppStmt, 5);

	char *deptype = (char *) sqlite3_column_text(query->ppStmt, 6);

	/* we have a single char deptype */
	dep->deptype = deptype[0];

	if (sqlite3_column_type(query->ppStmt, 7) != SQLITE_NULL)
	{
		strlcpy(dep->type,
				(char *) sqlite3_column_text(query->ppStmt, 7),
				sizeof(dep->type));
	}

	if (sqlite3_column_type(query->ppStmt, 8) != SQLITE_NULL)
	{
		strlcpy(dep->identity,
				(char *) sqlite3_column_text(query->ppStmt, 8),
				sizeof(dep->identity));
	}

	return true;
}


/*
 * catalog_iter_s_depend_finish cleans-up the internal memory used for the
 * iteration.
 */
bool
catalog_iter_s_depend_finish(SourceDependIterator *iter)
{
	SQLiteQuery *query = &(iter->query);

	/* in case we finish before reaching the DONE step */
	if (iter->dep != NULL)
	{
		iter->dep = NULL;
	}

	if (!catalog_sql_finalize(query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_upsert_process_info INSERTs or UPDATEs a process information entry
 * in our catalogs, allowing to keep track of what's happening.
 */
bool
catalog_upsert_process_info(DatabaseCatalog *catalog, ProcessInfo *ps)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_upsert_process_info: db is NULL");
		return false;
	}

	char *sql =
		"insert or replace into process("
		"  pid, ps_type, ps_title, tableoid, partnum, indexoid)"
		"values($1, $2, $3, $4, $5, $6)";

	if (!semaphore_lock(&(catalog->sema)))
	{
		/* errors have already been logged */
		return false;
	}

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "pid", (long long) ps->pid, NULL },
		{ BIND_PARAMETER_TYPE_TEXT, "ps_type", 0, ps->psType },
		{ BIND_PARAMETER_TYPE_TEXT, "ps_title", 0, ps->psTitle },
		{ BIND_PARAMETER_TYPE_INT64, "tableoid", ps->tableOid, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "partnum", ps->partNumber, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "indexoid", ps->indexOid, NULL }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* now execute the query, which does not return any row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	(void) semaphore_unlock(&(catalog->sema));

	return true;
}


/*
 * catalog_delete_s_table deletes an s_table entry for the given oid.
 */
bool
catalog_delete_process(DatabaseCatalog *catalog, pid_t pid)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_delete_process: db is NULL");
		return false;
	}

	char *sql = "delete from process where pid = $1";

	if (!semaphore_lock(&(catalog->sema)))
	{
		/* errors have already been logged */
		return false;
	}

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "pid", (long long) pid, NULL }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* now execute the query, which does not return any row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	(void) semaphore_unlock(&(catalog->sema));

	return true;
}


/*
 * catalog_iter_s_table_in_copy iterates over the list of tables with a COPY
 * process in our catalogs.
 */
bool
catalog_iter_s_table_in_copy(DatabaseCatalog *catalog,
							 void *context,
							 SourceTableIterFun *callback)
{
	SourceTableIterator *iter =
		(SourceTableIterator *) calloc(1, sizeof(SourceTableIterator));

	iter->catalog = catalog;

	if (!catalog_iter_s_table_in_copy_init(iter))
	{
		/* errors have already been logged */
		return false;
	}

	for (;;)
	{
		if (!catalog_iter_s_table_next(iter))
		{
			/* errors have already been logged */
			return false;
		}

		SourceTable *table = iter->table;

		if (table == NULL)
		{
			if (!catalog_iter_s_table_finish(iter))
			{
				/* errors have already been logged */
				return false;
			}

			break;
		}

		/* now call the provided callback */
		if (!(*callback)(context, table))
		{
			log_error("Failed to iterate over list of tables, "
					  "see above for details");
			return false;
		}
	}


	return true;
}


/*
 * catalog_iter_s_table_in_copy_init initializes an Interator over our catalog
 * of SourceTable entries.
 */
bool
catalog_iter_s_table_in_copy_init(SourceTableIterator *iter)
{
	sqlite3 *db = iter->catalog->db;

	if (db == NULL)
	{
		log_error("BUG: Failed to initialize s_table iterator: db is NULL");
		return false;
	}

	iter->table = (SourceTable *) calloc(1, sizeof(SourceTable));

	if (iter->table == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	char *sql =
		"  select t.oid, qname, nspname, relname, amname, restore_list_name, "
		"         relpages, reltuples, ts.bytes, ts.bytes_pretty, "
		"         exclude_data, part_key, "
		"         part.partcount, s.partnum, part.min, part.max "

		"    from process p "
		"         join s_table t on p.tableoid = t.oid "
		"         join summary s on s.pid = p.pid "
		"                       and s.tableoid = p.tableoid "

		"         left join s_table_part part "
		"                on part.oid = p.tableoid "
		"               and part.partnum = s.partnum "

		"         left join s_table_chksum c on c.oid = p.tableoid "
		"		  left join s_table_size ts on ts.oid = p.tableoid "
		"   where p.ps_type = 'COPY' "
		"order by p.pid";

	SQLiteQuery *query = &(iter->query);

	query->context = iter->table;
	query->fetchFunction = &catalog_s_table_fetch;

	if (!catalog_sql_prepare(db, sql, query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_iter_s_index_in_progress iterates over the list of indexs with a
 * CREATE INDEX process in our catalogs.
 */
bool
catalog_iter_s_index_in_progress(DatabaseCatalog *catalog,
								 void *context,
								 SourceIndexIterFun *callback)
{
	SourceIndexIterator *iter =
		(SourceIndexIterator *) calloc(1, sizeof(SourceIndexIterator));

	iter->catalog = catalog;

	if (!catalog_iter_s_index_in_progress_init(iter))
	{
		/* errors have already been logged */
		return false;
	}

	for (;;)
	{
		if (!catalog_iter_s_index_next(iter))
		{
			/* errors have already been logged */
			return false;
		}

		SourceIndex *index = iter->index;

		if (index == NULL)
		{
			if (!catalog_iter_s_index_finish(iter))
			{
				/* errors have already been logged */
				return false;
			}

			break;
		}

		/* now call the provided callback */
		if (!(*callback)(context, index))
		{
			log_error("Failed to iterate over list of indexs, "
					  "see above for details");
			return false;
		}
	}


	return true;
}


/*
 * catalog_iter_s_index_in_progress_init initializes an Interator over our
 * catalog of SourceIndex entries.
 */
bool
catalog_iter_s_index_in_progress_init(SourceIndexIterator *iter)
{
	sqlite3 *db = iter->catalog->db;

	if (db == NULL)
	{
		log_error("BUG: Failed to initialize s_index iterator: db is NULL");
		return false;
	}

	iter->index = (SourceIndex *) calloc(1, sizeof(SourceIndex));

	if (iter->index == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	char *sql =
		"  select i.oid, i.qname, i.nspname, i.relname, i.restore_list_name, "
		"         i.tableoid, t.qname, t.nspname, t.relname, "
		"         isprimary, isunique, columns, i.sql, "
		"         c.oid as constraintoid, conname, "
		"         condeferrable, condeferred, c.sql as condef"
		"    from process p "
		"         join s_index i on p.indexoid = i.oid "
		"         join s_table t on t.oid = i.tableoid "
		"         left join s_constraint c on c.indexoid = i.oid"
		"   where p.ps_type = 'CREATE INDEX'";

	SQLiteQuery *query = &(iter->query);

	query->context = iter->index;
	query->fetchFunction = &catalog_s_index_fetch;

	if (!catalog_sql_prepare(db, sql, query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_count_summary_done counts the number of tables and indexes that have
 * already been processed from the summary table.
 */
bool
catalog_count_summary_done(DatabaseCatalog *catalog,
						   CatalogProgressCount *count)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_count_summary_done: db is NULL");
		return false;
	}

	char *sql =
		"select "
		"  ("
		"    with pdone as "
		"    ("
		"     select tableoid, "
		"            count(s.partnum) as partdone, "
		"            coalesce(p.partcount, 1) as partcount "
		"       from summary s "
		"            join s_table t on t.oid = s.tableoid "
		"            left join s_table_part p on p.oid = t.oid and p.partnum = s.partnum "
		"      where tableoid is not null "
		"        and done_time_epoch is not null "
		"   group by tableoid"
		"    ) "
		"    select count(tableoid) from pdone where partdone = partcount"
		"  ) as tblcount,"
		"  ("
		"   select count(indexoid) "
		"     from summary "
		"    where indexoid is not null and done_time_epoch is not null"
		"  ) as idxcount";

	SQLiteQuery query = {
		.context = count,
		.fetchFunction = &catalog_count_summary_done_fetch
	};

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which return exactly one row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_count_summary_done_fetch fetches a CatalogProgressCount from a query
 * ppStmt result.
 */
bool
catalog_count_summary_done_fetch(SQLiteQuery *query)
{
	CatalogProgressCount *count = (CatalogProgressCount *) query->context;

	/* cleanup the memory area before re-use */
	bzero(count, sizeof(CatalogProgressCount));

	count->table = sqlite3_column_int64(query->ppStmt, 0);
	count->index = sqlite3_column_int64(query->ppStmt, 1);

	return true;
}


/*
 * catalog_add_timeline_history inserts a timeline history entry to our
 * internal catalogs database.
 */
bool
catalog_add_timeline_history(DatabaseCatalog *catalog, TimelineHistoryEntry *entry)
{
	if (catalog == NULL)
	{
		log_error("BUG: catalog_add_timeline_history: catalog is NULL");
		return false;
	}

	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_add_timeline_history: db is NULL");
		return false;
	}

	char *sql =
		"insert or replace into timeline_history(tli, startpos, endpos)"
		"values($1, $2, $3)";

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	char slsn[PG_LSN_MAXLENGTH] = { 0 };
	char elsn[PG_LSN_MAXLENGTH] = { 0 };

	sformat(slsn, sizeof(slsn), "%X/%X", LSN_FORMAT_ARGS(entry->begin));
	sformat(elsn, sizeof(elsn), "%X/%X", LSN_FORMAT_ARGS(entry->end));

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT, "tli", entry->tli, NULL },
		{ BIND_PARAMETER_TYPE_TEXT, "startpos", 0, slsn },
		{ BIND_PARAMETER_TYPE_TEXT, "endpos", 0, elsn }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which does not return any row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_lookup_timeline_history fetches the current TimelineHistoryEntry
 * from our catalogs.
 */
bool
catalog_lookup_timeline_history(DatabaseCatalog *catalog,
								int tli,
								TimelineHistoryEntry *entry)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_lookup_timeline_history: db is NULL");
		return false;
	}

	SQLiteQuery query = {
		.context = entry,
		.fetchFunction = &catalog_timeline_history_fetch
	};

	char *sql =
		"  select tli, startpos, endpos"
		"    from timeline_history"
		"   where tli = $1";


	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT, "tli", tli, NULL }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which return exactly one row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * catalog_timeline_history_fetch fetches a TimelineHistoryEntry from a query
 * ppStmt result.
 */
bool
catalog_timeline_history_fetch(SQLiteQuery *query)
{
	TimelineHistoryEntry *entry = (TimelineHistoryEntry *) query->context;

	bzero(entry, sizeof(TimelineHistoryEntry));

	/* tli */
	entry->tli = sqlite3_column_int(query->ppStmt, 0);

	/* begin LSN */
	if (sqlite3_column_type(query->ppStmt, 1) != SQLITE_NULL)
	{
		const char *startpos = (const char *) sqlite3_column_text(query->ppStmt, 1);

		if (!parseLSN(startpos, &entry->begin))
		{
			log_error("Failed to parse LSN from \"%s\"", startpos);
			return false;
		}
	}

	/* end LSN */
	if (sqlite3_column_type(query->ppStmt, 2) != SQLITE_NULL)
	{
		const char *endpos = (const char *) sqlite3_column_text(query->ppStmt, 2);

		if (!parseLSN(endpos, &entry->end))
		{
			log_error("Failed to parse LSN from \"%s\"", endpos);
			return false;
		}
	}

	return true;
}


/*
 * catalog_execute executes sqlite query
 */
bool
catalog_execute(DatabaseCatalog *catalog, char *sql)
{
	log_sqlite("[SQLite] %s", sql);

	int rc = sqlite3_exec(catalog->db, sql, NULL, NULL, NULL);

	if (rc != SQLITE_OK)
	{
		log_error("[SQLite]: %s failed: %s", sql, sqlite3_errstr(rc));
		return false;
	}

	return true;
}


/*
 * catalog_sql_prepare prepares a SQLite query for our internal catalogs.
 */
bool
catalog_sql_prepare(sqlite3 *db, const char *sql, SQLiteQuery *query)
{
	query->db = db;
	query->sql = sql;

	log_sqlite("[SQLite] %s", sql);

	int rc = sqlite3_prepare_v2(db, sql, -1, &(query->ppStmt), NULL);

	if (rc == SQLITE_LOCKED || rc == SQLITE_BUSY)
	{
		ConnectionRetryPolicy retryPolicy = { 0 };

		int maxT = 5;            /* 5s */
		int maxSleepTime = 150;  /* 150ms */
		int baseSleepTime = 10;  /* 10ms */

		(void) pgsql_set_retry_policy(&retryPolicy,
									  maxT,
									  -1, /* unbounded number of attempts */
									  maxSleepTime,
									  baseSleepTime);

		while ((rc == SQLITE_LOCKED || rc == SQLITE_BUSY) &&
			   !pgsql_retry_policy_expired(&retryPolicy))
		{
			int sleepTimeMs =
				pgsql_compute_connection_retry_sleep_time(&retryPolicy);

			log_sqlite("[SQLite %d]: %s, try again in %dms",
					   rc,
					   sqlite3_errstr(rc),
					   sleepTimeMs);

			/* we have milliseconds, pg_usleep() wants microseconds */
			(void) pg_usleep(sleepTimeMs * 1000);

			rc = sqlite3_prepare_v2(db, sql, -1, &(query->ppStmt), NULL);
		}
	}

	if (rc != SQLITE_OK || query->ppStmt == NULL)
	{
		log_error("Failed to prepare SQLite statement: %s", query->sql);
		log_error("[SQLite] %s", sqlite3_errmsg(query->db));
		return false;
	}

	return true;
}


/*
 * catalog_sql_bind binds parameters to our SQL query before execution.
 */
bool
catalog_sql_bind(SQLiteQuery *query, BindParam *params, int count)
{
	if (!catalog_bind_parameters(query->db, query->ppStmt, params, count))
	{
		/* errors have already been logged */
		(void) sqlite3_clear_bindings(query->ppStmt);
		(void) sqlite3_finalize(query->ppStmt);
		return false;
	}

	return true;
}


/*
 * catalog_sql_execute_once executes a query once and fetches its results.
 */
bool
catalog_sql_execute_once(SQLiteQuery *query)
{
	if (!catalog_sql_execute(query))
	{
		log_error("Failed to execute SQLite query, see above for details");
		return false;
	}

	if (!catalog_sql_finalize(query))
	{
		log_error("Failed to finalize SQLite query, see above for details");
		return false;
	}

	return true;
}


/*
 * catalog_sql_execute executes a query and fetches its results.
 */
bool
catalog_sql_execute(SQLiteQuery *query)
{
	/* we expect SQLITE_DONE when we don't have a fetchFunction callback */
	if (query->fetchFunction == NULL)
	{
		int rc = catalog_sql_step(query);

		if (rc != SQLITE_DONE)
		{
			log_error("Failed to execute statement: %s", query->sql);
			log_error("[SQLite %d] %s", rc, sqlite3_errstr(rc));

			(void) sqlite3_clear_bindings(query->ppStmt);
			(void) sqlite3_finalize(query->ppStmt);

			return false;
		}
	}
	/* when we have a fetchFunction we expect only one row, and exactly one */
	else
	{
		int rc = catalog_sql_step(query);

		if (rc == SQLITE_DONE)
		{
			if (query->errorOnZeroRows)
			{
				log_error("SQLite query returned 0 row: %s", query->sql);
				return false;
			}
		}
		else
		{
			if (rc != SQLITE_ROW)
			{
				log_error("Failed to step through statement: %s", query->sql);
				log_error("[SQLite %d] %s", rc, sqlite3_errstr(rc));

				(void) sqlite3_clear_bindings(query->ppStmt);
				(void) sqlite3_finalize(query->ppStmt);

				return false;
			}

			/* callback */
			if (!query->fetchFunction(query))
			{
				log_error("Failed to fetch current row, "
						  "see above for details");
				(void) sqlite3_clear_bindings(query->ppStmt);
				(void) sqlite3_finalize(query->ppStmt);
				return false;
			}

			if (catalog_sql_step(query) != SQLITE_DONE)
			{
				log_error("Failed to execute statement: %s", query->sql);
				log_error("[SQLite %d] %s", rc, sqlite3_errstr(rc));

				(void) sqlite3_clear_bindings(query->ppStmt);
				(void) sqlite3_finalize(query->ppStmt);

				return false;
			}
		}
	}

	/* clean-up after execute */
	int rc = sqlite3_clear_bindings(query->ppStmt);

	if (rc != SQLITE_OK)
	{
		log_error("Failed to clear SQLite bindings: %s", sqlite3_errstr(rc));
		return false;
	}

	/* reset the prepared Statement too */
	rc = sqlite3_reset(query->ppStmt);

	if (rc != SQLITE_OK)
	{
		log_error("Failed to reset SQLite statement: %s", sqlite3_errstr(rc));
		return false;
	}

	return true;
}


/*
 * catalog_sql_step is a wrapper around sqlite3_step() that implements a retry
 * policy when the return code is SQLITE_LOCKED or SQLITE_BUSY, allowing for
 * hanlding concurrent accesses between our sub-processes.
 */
int
catalog_sql_step(SQLiteQuery *query)
{
	int rc = sqlite3_step(query->ppStmt);

	if (rc == SQLITE_LOCKED || rc == SQLITE_BUSY)
	{
		ConnectionRetryPolicy retryPolicy = { 0 };

		int maxT = 5;            /* 5s */
		int maxSleepTime = 350;  /* 350ms */
		int baseSleepTime = 10;  /* 10ms */

		(void) pgsql_set_retry_policy(&retryPolicy,
									  maxT,
									  -1, /* unbounded number of attempts */
									  maxSleepTime,
									  baseSleepTime);

		while ((rc == SQLITE_LOCKED || rc == SQLITE_BUSY) &&
			   !pgsql_retry_policy_expired(&retryPolicy))
		{
			int sleepTimeMs =
				pgsql_compute_connection_retry_sleep_time(&retryPolicy);

			log_sqlite("[SQLite %d]: %s, try again in %dms",
					   rc,
					   sqlite3_errmsg(query->db),
					   sleepTimeMs);

			/* we have milliseconds, pg_usleep() wants microseconds */
			(void) pg_usleep(sleepTimeMs * 1000);

			rc = sqlite3_step(query->ppStmt);
		}
	}

	return rc;
}


/*
 * catalog_sql_finalize finalizes a SQL query.
 */
bool
catalog_sql_finalize(SQLiteQuery *query)
{
	if (sqlite3_finalize(query->ppStmt) != SQLITE_OK)
	{
		log_error("Failed to finalize SQLite statement: %s",
				  sqlite3_errmsg(query->db));
		return false;
	}

	return true;
}


/*
 * catalog_bind_parameters binds parameters to a SQLite prepared statement.
 */
bool
catalog_bind_parameters(sqlite3 *db,
						sqlite3_stmt *ppStmt,
						BindParam *params,
						int count)
{
	PQExpBuffer debugParameters = NULL;

	bool logSQL = log_get_level() <= LOG_SQLITE;

	if (logSQL)
	{
		debugParameters = createPQExpBuffer();
	}

	for (int i = 0; i < count; i++)
	{
		int n = i + 1;
		BindParam *p = &(params[i]);

		if (logSQL && i > 0)
		{
			appendPQExpBufferStr(debugParameters, ", ");
		}

		switch (p->type)
		{
			case BIND_PARAMETER_TYPE_INT:
			{
				int rc = sqlite3_bind_int(ppStmt, n, p->intVal);

				if (rc != SQLITE_OK)
				{
					log_error("[SQLite %d] Failed to bind \"%s\" value %lld: %s",
							  rc,
							  p->name,
							  (long long) p->intVal,
							  sqlite3_errstr(rc));
					return false;
				}

				if (logSQL)
				{
					appendPQExpBuffer(debugParameters, "%lld", (long long) p->intVal);
				}

				break;
			}

			case BIND_PARAMETER_TYPE_INT64:
			{
				int rc = sqlite3_bind_int64(ppStmt, n, p->intVal);

				if (rc != SQLITE_OK)
				{
					log_error("[SQLite %d] Failed to bind \"%s\" value %lld: %s",
							  rc,
							  p->name,
							  (long long) p->intVal,
							  sqlite3_errstr(rc));
					return false;
				}

				if (logSQL)
				{
					appendPQExpBuffer(debugParameters, "%lld", (long long) p->intVal);
				}

				break;
			}

			case BIND_PARAMETER_TYPE_TEXT:
			{
				int rc;

				/* deal with empty string[] buffers same as NULL pointers */
				if (p->strVal == NULL ||
					IS_EMPTY_STRING_BUFFER(p->strVal))
				{
					rc = sqlite3_bind_null(ppStmt, n);

					if (logSQL)
					{
						appendPQExpBuffer(debugParameters, "%s", "null");
					}
				}
				else
				{
					rc = sqlite3_bind_text(ppStmt,
										   n,
										   p->strVal,
										   strlen(p->strVal),
										   SQLITE_STATIC);
					if (logSQL)
					{
						appendPQExpBuffer(debugParameters, "%s", p->strVal);
					}
				}

				if (rc != SQLITE_OK)
				{
					log_error("[SQLite %d] Failed to bind \"%s\" value \"%s\": %s",
							  rc,
							  p->name,
							  p->strVal,
							  sqlite3_errstr(rc));
					return false;
				}

				break;
			}

			default:
			{
				log_error("BUG: catalog_bind_parameters called with unknown "
						  "parameter type %d",
						  p->type);
				return false;
			}
		}
	}


	if (logSQL)
	{
		if (PQExpBufferBroken(debugParameters))
		{
			log_error("Failed to create log message for SQL query parameters: "
					  "out of memory");
			destroyPQExpBuffer(debugParameters);
			return false;
		}

		log_sqlite("[SQLite] %s", debugParameters->data);

		destroyPQExpBuffer(debugParameters);
	}

	return true;
}


/*
 * catalog_start_timing starts our timing.
 */
void
catalog_start_timing(TopLevelTiming *timing)
{
	timing->startTime = time(NULL);
	INSTR_TIME_SET_CURRENT(timing->startTimeInstr);
}


/*
 * catalog_start_timing stops our timing and compute the duration in
 * milliseconds.
 */
void
catalog_stop_timing(TopLevelTiming *timing)
{
	timing->doneTime = time(NULL);

	/* cumulative timings increment their duration separately */
	if (!timing->cumulative)
	{
		INSTR_TIME_SET_CURRENT(timing->durationInstr);
		INSTR_TIME_SUBTRACT(timing->durationInstr, timing->startTimeInstr);

		timing->durationMs = INSTR_TIME_GET_MILLISEC(timing->durationInstr);

		IntervalToString(timing->durationMs,
						 timing->ppDuration,
						 INTSTRING_MAX_DIGITS);
	}
}


/*
 * catalog_iter_s_table_generated_columns iterates over the list of tables that
 * have a generated columns in our catalogs.
 */
bool
catalog_iter_s_table_generated_columns(DatabaseCatalog *catalog,
									   void *context,
									   SourceTableIterFun *callback)
{
	SourceTableIterator *iter =
		(SourceTableIterator *) calloc(1, sizeof(SourceTableIterator));

	iter->catalog = catalog;

	if (!catalog_iter_s_table_generated_columns_init(iter))
	{
		/* errors have already been logged */
		return false;
	}

	for (;;)
	{
		if (!catalog_iter_s_table_next(iter))
		{
			/* errors have already been logged */
			return false;
		}

		SourceTable *table = iter->table;

		if (table == NULL)
		{
			if (!catalog_iter_s_table_finish(iter))
			{
				/* errors have already been logged */
				return false;
			}

			break;
		}

		/* now call the provided callback */
		if (!(*callback)(context, table))
		{
			log_error("Failed to iterate over list of tables, "
					  "see above for details");
			return false;
		}
	}


	return true;
}


/*
 * catalog_iter_s_table_generated_columns_init initializes an Interator over our
 * catalog of SourceTable entries which has generated columns.
 */
bool
catalog_iter_s_table_generated_columns_init(SourceTableIterator *iter)
{
	sqlite3 *db = iter->catalog->db;

	if (db == NULL)
	{
		log_error(
			"BUG: Failed to initialize catalog_iter_s_table_generated_columns_init iterator: db is NULL");
		return false;
	}

	iter->table = (SourceTable *) calloc(1, sizeof(SourceTable));

	if (iter->table == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	char *sql =
		"  select t.oid, qname, nspname, relname, amname, restore_list_name, "
		"         relpages, reltuples, ts.bytes, ts.bytes_pretty, "
		"         exclude_data, part_key, "
		"         (select count(1) from s_table_part p where p.oid = t.oid) "
		"    from s_table t join s_attr a "

		/*
		 * Currently, we handle only:
		 * - Generated columns with is_generated = 'ALWAYS' for INSERT and UPDATE
		 * - IDENTITY columns for INSERT using "overriding system value"
		 *
		 * TODO: Add support for IDENTITY columns in UPDATE.
		 * https://github.com/dimitri/pgcopydb/issues/844
		 */
		"       on (a.oid = t.oid and a.attisgenerated = 1) "
		"       left join s_table_size ts on ts.oid = t.oid "
		"group by t.oid "
		"  having sum(a.attisgenerated) > 0 "
		"order by bytes desc";

	SQLiteQuery *query = &(iter->query);

	query->context = iter->table;
	query->fetchFunction = &catalog_s_table_fetch;

	if (!catalog_sql_prepare(db, sql, query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}
