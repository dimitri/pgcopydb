/*
 * src/bin/pgcopydb/copydb_schema.c
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <sys/wait.h>
#include <unistd.h>

#include "catalog.h"
#include "copydb.h"
#include "env_utils.h"
#include "filtering.h"
#include "lock_utils.h"
#include "log.h"
#include "pidfile.h"
#include "schema.h"
#include "signals.h"
#include "string_utils.h"
#include "summary.h"

static bool copydb_fetch_source_catalog_setup(CopyDataSpec *specs);
static bool copydb_fetch_previous_run_state(CopyDataSpec *specs);
static bool copydb_fetch_source_schema(CopyDataSpec *specs, PGSQL *src);

static bool copydb_prepare_table_specs_hook(void *ctx, SourceTable *source);


/*
 * copydb_fetch_schema_and_prepare_specs fetches the list of tables from the
 * source database, and then fetches the list of objects that are filtered-out
 * (schemas, tables, indexes, constraints, then objects that depend on those).
 *
 * Then the per-table CopyTableDataSpec are initialized in preparation of the
 * rest of the work.
 */
bool
copydb_fetch_schema_and_prepare_specs(CopyDataSpec *specs)
{
	if (!copydb_fetch_source_catalog_setup(specs))
	{
		/* errors have already been logged */
		return false;
	}

	if (!specs->fetchCatalogs)
	{
		log_info("Re-using catalog caches");
		return true;
	}

	if (!summary_start_timing(&(specs->catalogs.source),
							  TIMING_SECTION_CATALOG_QUERIES))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * Either use the already established connection and transaction that
	 * exports our snapshot in the main process, or establish a transaction
	 * that groups together the filters preparation in temp tables and then the
	 * queries that join with those temp tables.
	 */
	PGSQL *src = NULL;
	PGSQL pgsql = { 0 };

	bool preparedSnapshot = false;

	if (specs->resume && specs->consistent)
	{
		log_debug("re-use snapshot \"%s\"", specs->sourceSnapshot.snapshot);

		if (IS_EMPTY_STRING_BUFFER(specs->sourceSnapshot.snapshot))
		{
			log_error("Failed to re-use snapshot \"%s\"",
					  specs->sourceSnapshot.snapshot);
			return false;
		}

		/* we might have to prepare the snapshot locally */
		if (specs->sourceSnapshot.state == SNAPSHOT_STATE_UNKNOWN)
		{
			if (!copydb_prepare_snapshot(specs))
			{
				log_error("Failed to re-use snapshot \"%s\", see above for details",
						  specs->sourceSnapshot.snapshot);
				return false;
			}

			preparedSnapshot = true;
		}

		src = &(specs->sourceSnapshot.pgsql);
	}
	else
	{
		log_debug("--not-consistent, create a fresh connection");
		if (!pgsql_init(&pgsql, specs->connStrings.source_pguri, PGSQL_CONN_SOURCE))
		{
			/* errors have already been logged */
			return false;
		}

		src = &pgsql;

		if (!pgsql_begin(src))
		{
			/* errors have already been logged */
			return false;
		}
	}

	/* make sure we receive only one row at a time in-memory */
	src->singleRowMode = true;

	if (!copydb_fetch_source_schema(specs, src))
	{
		/* errors have already been logged */
		return false;
	}

	/* time to finish the transaction on the source database */
	if (preparedSnapshot)
	{
		if (!copydb_close_snapshot(specs))
		{
			/* errors have already been logged */
			return false;
		}
	}
	else
	{
		if (!pgsql_commit(src))
		{
			/* errors have already been logged */
			return false;
		}
	}

	/*
	 * Now fetch the list of schemas and roles found in the target database.
	 * The information is needed to fetch related database properties
	 * (settings) when set to a specific role within that database.
	 */
	if (specs->section == DATA_SECTION_ALL ||
		specs->section == DATA_SECTION_EXTENSIONS ||
		specs->section == DATA_SECTION_COLLATIONS)
	{
		if (!copydb_prepare_target_catalog(specs))
		{
			/* errors have already been logged */
			return false;
		}
	}

	/*
	 * The catalog totalDurationMs has been fetched from the previous state of
	 * the cache in copydb_fetch_source_catalog_setup, update the value now.
	 */
	if (!catalog_total_duration(&(specs->catalogs.source)) ||
		!catalog_total_duration(&(specs->catalogs.filter)))
	{
		/* errors have already been logged */
		return false;
	}

	if (!summary_stop_timing(&(specs->catalogs.source),
							 TIMING_SECTION_CATALOG_QUERIES))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * copydb_fetch_source_catalog_setup initializes our local catalog cache and
 * checks th setup and cache state.
 */
static bool
copydb_fetch_source_catalog_setup(CopyDataSpec *specs)
{
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);
	DatabaseCatalog *filtersDB = &(specs->catalogs.filter);

	/*
	 * We might just re-use the existing cache, or we might want to do
	 * cache-invalidation.
	 */
	if (!catalog_init_from_specs(specs))
	{
		log_error("Failed to initialize pgcopydb internal catalogs");
		return false;
	}

	if (!copydb_fetch_previous_run_state(specs))
	{
		log_error("Failed to fetch catalog state from a potential previous run");
		return false;
	}

	/*
	 * Now see if the cache has already been filled or if we need to connect to
	 * the source and fetch the data again. By default, set fetchCatalogs to
	 * true to force cache invalidation.
	 */
	specs->fetchCatalogs = true;

	bool allDone = true;
	sourceDB->totalDurationMs = 0;

	/* skip DATA_SECTION_NONE (hard-coded to enum value 0) */
	for (int i = 1; i < DATA_SECTION_COUNT; i++)
	{
		CatalogSection *s = &(sourceDB->sections[i]);

		/* use the enum value as the sections array index */
		s->section = (CopyDataSection) i;

		if (!catalog_section_state(sourceDB, s))
		{
			/* errors have already been logged */
			return false;
		}

		sourceDB->totalDurationMs += s->durationMs;

		/* compute "allDone" in the context of a sourceDB */
		if (s->section == DATA_SECTION_DATABASE_PROPERTIES ||
			s->section == DATA_SECTION_TABLE_DATA ||
			s->section == DATA_SECTION_SET_SEQUENCES ||
			s->section == DATA_SECTION_INDEXES ||
			s->section == DATA_SECTION_CONSTRAINTS)
		{
			allDone = allDone && s->fetched;
		}

		/* ignore "parts" unless --split-tables-larger-than has been used */
		if (sourceDB->setup.splitTablesLargerThanBytes > 0)
		{
			if (s->section == DATA_SECTION_TABLE_DATA_PARTS)
			{
				allDone = allDone && s->fetched;
			}
		}
	}

	/* compute "allDone" in the context of the filtersDB too */
	filtersDB->totalDurationMs = 0;

	if (specs->fetchFilteredOids)
	{
		/* skip DATA_SECTION_NONE (hard-coded to enum value 0) */
		for (int i = 1; i < DATA_SECTION_COUNT; i++)
		{
			CatalogSection *s = &(filtersDB->sections[i]);

			/* use the enum value as the sections array index */
			s->section = (CopyDataSection) i;

			if (!catalog_section_state(filtersDB, s))
			{
				/* errors have already been logged */
				return false;
			}

			filtersDB->totalDurationMs += s->durationMs;

			/* compute "allDone" in the context of a sourceDB */
			if (s->section == DATA_SECTION_COLLATIONS ||
				s->section == DATA_SECTION_EXTENSIONS ||
				s->section == DATA_SECTION_TABLE_DATA ||
				s->section == DATA_SECTION_SET_SEQUENCES ||
				s->section == DATA_SECTION_INDEXES ||
				s->section == DATA_SECTION_CONSTRAINTS ||
				s->section == DATA_SECTION_DEPENDS ||
				s->section == DATA_SECTION_FILTERS)
			{
				allDone = allDone && s->fetched;
			}
		}
	}

	if (allDone)
	{
		specs->fetchCatalogs = false;
		return true;
	}

	/*
	 * Subcommands need only a subpart of the catalogs.
	 *
	 * Some commands access the filtersDB catalog only:
	 *
	 *  - pgcopydb list collations
	 *  - pgcopydb list extensions
	 *  - pgcopydb list depends
	 */
	if (specs->section != DATA_SECTION_ALL)
	{
		if (specs->section == DATA_SECTION_COLLATIONS ||
			specs->section == DATA_SECTION_EXTENSIONS ||
			specs->section == DATA_SECTION_DEPENDS)
		{
			specs->fetchCatalogs = !filtersDB->sections[specs->section].fetched;
		}
		else
		{
			specs->fetchCatalogs = !sourceDB->sections[specs->section].fetched;
		}

		/*
		 * Special case for commands that need to fetchFilteredOids and use the
		 * --skip-extension or --skip-collations options.
		 */
		if (specs->fetchFilteredOids)
		{
			if (specs->skipExtensions)
			{
				specs->fetchCatalogs =
					specs->fetchCatalogs &&
					!sourceDB->sections[DATA_SECTION_EXTENSIONS].fetched;
			}

			if (specs->skipCollations)
			{
				specs->fetchCatalogs =
					specs->fetchCatalogs &&
					!sourceDB->sections[DATA_SECTION_COLLATIONS].fetched;
			}
		}
	}

	return true;
}


/*
 * copydb_fetch_previous_run_state inspects a potential previous run state.
 */
static bool
copydb_fetch_previous_run_state(CopyDataSpec *specs)
{
	/*
	 * See if previous work was done already, by using the timings
	 * done_time_epoch columns of the Top-Level Timings in the catalogs.
	 */
	if (!summary_prepare_toplevel_durations(specs))
	{
		/* errors have already been logged */
		return false;
	}

	if (topLevelTimingArray[TIMING_SECTION_TOTAL].doneTime > 0)
	{
		specs->runState.allDone = true;
		log_info("A previous run has run through completion");
	}

	if (topLevelTimingArray[TIMING_SECTION_DUMP_SCHEMA].doneTime > 0)
	{
		specs->runState.schemaDumpIsDone = true;
		log_notice("Schema dump for pre-data and post-data have been done");
	}

	if (topLevelTimingArray[TIMING_SECTION_PREPARE_SCHEMA].doneTime > 0)
	{
		specs->runState.schemaPreDataHasBeenRestored = true;
		log_notice("Pre-data schema has been restored on the target instance");
	}

	if (topLevelTimingArray[TIMING_SECTION_COPY_DATA].doneTime > 0)
	{
		specs->runState.tableCopyIsDone = true;
		log_notice("Table Data has been copied to the target instance");
	}

	if (topLevelTimingArray[TIMING_SECTION_CREATE_INDEX].doneTime > 0)
	{
		specs->runState.indexCopyIsDone = true;
		log_notice("Indexes have been copied to the target instance");
	}

	if (topLevelTimingArray[TIMING_SECTION_SET_SEQUENCES].doneTime > 0)
	{
		specs->runState.sequenceCopyIsDone = true;
		log_notice("Sequences have been copied to the target instance");
	}

	if (topLevelTimingArray[TIMING_SECTION_LARGE_OBJECTS].doneTime > 0)
	{
		specs->runState.blobsCopyIsDone = true;
		log_notice("Large Objects have been copied to the target instance");
	}

	if (topLevelTimingArray[TIMING_SECTION_FINALIZE_SCHEMA].doneTime > 0)
	{
		specs->runState.schemaPostDataHasBeenRestored = true;
		log_notice("Post-data schema has been restored on the target instance");
	}

	return true;
}


/*
 * copydb_fetch_source_schema is a utility function for the previous definition
 * copydb_fetch_schema_and_prepare_specs.
 */
static bool
copydb_fetch_source_schema(CopyDataSpec *specs, PGSQL *src)
{
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	/* check if we're connected to a standby server, which we don't support */
	bool pg_is_in_recovery = false;

	if (!pgsql_is_in_recovery(src, &pg_is_in_recovery))
	{
		/* errors have already been logged */
		return false;
	}

	if (pg_is_in_recovery)
	{
		log_fatal("Connected to a standby server where pg_is_in_recovery(): "
				  "pgcopydb does not support operating on standby server "
				  "at this point, as it needs to create temp tables");
		return false;
	}

	/* check if we have needed privileges here */
	if (!schema_query_privileges(src,
								 &(specs->hasDBCreatePrivilege),
								 &(specs->hasDBTempPrivilege)))
	{
		log_error("Failed to query database privileges, see above for details");
		return false;
	}

	if (!specs->hasDBTempPrivilege)
	{
		log_fatal("Connecting with a role that does not have TEMP privileges "
				  "on the current database on the source server");
		return false;
	}

	if (specs->hasDBCreatePrivilege)
	{
		if (!pgsql_prepend_search_path(src, "pgcopydb"))
		{
			/* errors have already been logged */
			return false;
		}
	}
	else
	{
		log_warn("Connecting with a role that does not have CREATE privileges "
				 "on the source database discards pg_table_size() caching");
	}

	/*
	 * Grab the source database properties to be able to install them again on
	 * the target, using ALTER DATABASE SET or ALTER USER IN DATABASE SET.
	 */
	if ((specs->section == DATA_SECTION_ALL ||
		 specs->section == DATA_SECTION_DATABASE_PROPERTIES) &&
		!sourceDB->sections[DATA_SECTION_DATABASE_PROPERTIES].fetched)
	{
		TopLevelTiming timing = {
			.label = CopyDataSectionToString(DATA_SECTION_DATABASE_PROPERTIES)
		};

		(void) catalog_start_timing(&timing);

		if (!schema_list_database_properties(src, sourceDB))
		{
			log_error("Failed to fetch database properties, "
					  "see above for details");
			return false;
		}

		(void) catalog_stop_timing(&timing);

		if (!catalog_register_section(sourceDB, &timing))
		{
			/* errors have already been logged */
			return false;
		}
	}

	if (!catalog_begin(sourceDB, false))
	{
		/* errors have already been logged */
		return false;
	}

	/* now fetch the list of tables from the source database */
	if ((specs->section == DATA_SECTION_ALL ||
		 specs->section == DATA_SECTION_TABLE_DATA ||
		 specs->section == DATA_SECTION_TABLE_DATA_PARTS) &&
		!sourceDB->sections[DATA_SECTION_TABLE_DATA].fetched)
	{
		/* copydb_fetch_filtered_oids() needs the table size table around */
		if (!schema_prepare_pgcopydb_table_size(src,
												&(specs->filters),
												sourceDB))
		{
			/* errors have already been logged */
			return false;
		}

		if (!copydb_prepare_table_specs(specs, src))
		{
			/* errors have already been logged */
			return false;
		}
	}

	/* fetch the list of all the indexes that are going to be created again */
	if ((specs->section == DATA_SECTION_ALL ||
		 specs->section == DATA_SECTION_INDEXES ||
		 specs->section == DATA_SECTION_CONSTRAINTS) &&
		!sourceDB->sections[DATA_SECTION_INDEXES].fetched)
	{
		if (!copydb_prepare_index_specs(specs, src))
		{
			/* errors have already been logged */
			return false;
		}
	}

	if ((specs->section == DATA_SECTION_ALL ||
		 specs->section == DATA_SECTION_SET_SEQUENCES) &&
		!sourceDB->sections[DATA_SECTION_SET_SEQUENCES].fetched)
	{
		if (!copydb_prepare_sequence_specs(specs, src))
		{
			/* errors have already been logged */
			return false;
		}
	}

	/* now update --split-tables-larger-than and target pguri */
	if (!catalog_update_setup(specs))
	{
		/* errors have already been logged */
		return false;
	}

	/* prepare the Oids of objects that are filtered out */
	if (specs->fetchFilteredOids)
	{
		if (!copydb_fetch_filtered_oids(specs, src))
		{
			/* errors have already been logged */
			return false;
		}
	}

	if (!catalog_commit(sourceDB))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


typedef struct PrepareTableSpecsContext
{
	CopyDataSpec *specs;
	PGSQL *pgsql;
} PrepareTableSpecsContext;


/*
 * copydb_prepare_table_specs fetches the list of tables to COPY data from the
 * source and into the target, and initialises our internal
 * CopyTableDataSpecsArray to drive the operations.
 */
bool
copydb_prepare_table_specs(CopyDataSpec *specs, PGSQL *pgsql)
{
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);
	SourceFilters *filters = &(specs->filters);

	TopLevelTiming timing = {
		.label = CopyDataSectionToString(DATA_SECTION_TABLE_DATA)
	};

	(void) catalog_start_timing(&timing);

	/*
	 * Now get the list of the tables we want to COPY over.
	 */
	if (!schema_list_ordinary_tables(pgsql, filters, sourceDB))
	{
		log_error("Failed to prepare table specs in our catalogs, "
				  "see above for details");
		return false;
	}

	(void) catalog_stop_timing(&timing);

	if (!catalog_register_section(sourceDB, &timing))
	{
		/* errors have already been logged */
		return false;
	}

	if (specs->splitTablesLargerThan.bytes > 0)
	{
		log_info("Splitting source candidate tables larger than %s",
				 specs->splitTablesLargerThan.bytesPretty);

		TopLevelTiming partsTiming = {
			.label = CopyDataSectionToString(DATA_SECTION_TABLE_DATA_PARTS)
		};

		(void) catalog_start_timing(&partsTiming);

		PrepareTableSpecsContext context = {
			.specs = specs,
			.pgsql = pgsql
		};

		if (!catalog_iter_s_table(sourceDB,
								  &context,
								  &copydb_prepare_table_specs_hook))
		{
			log_error("Failed to prepare table specs from internal catalogs, "
					  "see above for details");
			return false;
		}

		(void) catalog_stop_timing(&partsTiming);

		if (!catalog_register_section(sourceDB, &partsTiming))
		{
			/* errors have already been logged */
			return false;
		}
	}

	/*
	 * Now display some statistics about the COPY partitioning plan that we
	 * just computed.
	 */
	CatalogTableStats stats = { 0 };

	if (!catalog_s_table_stats(sourceDB, &stats))
	{
		log_error("Failed to compute source table statistics, "
				  "see above for details");
		return false;
	}

	log_info("Fetched information for %lld tables "
			 "(including %lld tables split in %lld partitions total), "
			 "with an estimated total of %s tuples and %s on-disk",
			 (long long) stats.count,
			 (long long) stats.countSplits,
			 (long long) stats.countParts,
			 stats.relTuplesPretty,
			 stats.bytesPretty);

	return true;
}


/*
 * copydb_prepare_table_specs_hook is an iterator callback function.
 */
static bool
copydb_prepare_table_specs_hook(void *ctx, SourceTable *source)
{
	PrepareTableSpecsContext *context = (PrepareTableSpecsContext *) ctx;
	CopyDataSpec *specs = context->specs;
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	if (specs->splitTablesLargerThan.bytes > 0 &&
		source->bytes < specs->splitTablesLargerThan.bytes)
	{
		return true;
	}

	/*
	 * Now compute partition scheme for same-table COPY concurrency, either
	 * using a integer field that is unique, or relying on CTID range scans
	 * otherwise.
	 *
	 * When the Table Access Method used is not "heap" we don't know if the
	 * CTID range scan is supported (see columnar storage extensions), so
	 * we skip partitioning altogether in that case.
	 */
	if (IS_EMPTY_STRING_BUFFER(source->partKey) &&
		streq(source->amname, "heap"))
	{
		log_info("Table %s is %s large "
				 "which is larger than --split-tables-larger-than %s, "
				 "and does not have a unique column of type integer: "
				 "splitting by CTID",
				 source->qname,
				 source->bytesPretty,
				 specs->splitTablesLargerThan.bytesPretty);

		strlcpy(source->partKey, "ctid", sizeof(source->partKey));

		/*
		 * Make sure we have proper statistics (relpages) about the table
		 * before compute the CTID ranges for the concurrent table scans.
		 */
		char sql[BUFSIZE] = { 0 };

		sformat(sql, sizeof(sql), "ANALYZE %s", source->qname);

		log_notice("%s", sql);

		if (!pgsql_execute(context->pgsql, sql))
		{
			log_error("Failed to refresh table %s statistics",
					  source->qname);
			return false;
		}

		/* fetch the relpages for the table after ANALYZE */
		if (!schema_list_relpages(context->pgsql, source, sourceDB))
		{
			log_error("Failed to fetch table %s relpages",
					  source->qname);
			return false;
		}
	}
	else if (!streq(source->amname, "heap"))
	{
		log_info("Table %s is %s large "
				 "which is larger than --split-tables-larger-than %s, "
				 "does not have a unique column of type integer, "
				 "and uses table access method \"%s\": "
				 "same table concurrency is not enabled",
				 source->qname,
				 source->bytesPretty,
				 specs->splitTablesLargerThan.bytesPretty,
				 source->amname);

		return true;
	}

	/*
	 * The schema_list_partitions() function queries the source database
	 * for partition ranges depending on the size of the source table and
	 * the range of unique key numbers (or CTID), and also fills-in our
	 * internal catalogs s_table_part.
	 */
	if (!schema_list_partitions(context->pgsql,
								sourceDB,
								source,
								specs->splitTablesLargerThan.bytes))
	{
		/* errors have already been logged */
		return false;
	}

	if (source->partition.partCount > 1)
	{
		log_info("Table %s is %s large, "
				 "%d COPY processes will be used, partitioning on %s.",
				 source->qname,
				 source->bytesPretty,
				 source->partition.partCount,
				 source->partKey);
	}

	return true;
}


/*
 * copydb_prepare_index_specs fetches the list of indexes to create again on
 * the target database, and set our internal hash table entries with a
 * linked-list of indexes per-table.
 */
bool
copydb_prepare_index_specs(CopyDataSpec *specs, PGSQL *pgsql)
{
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	TopLevelTiming timing = {
		.label = CopyDataSectionToString(DATA_SECTION_INDEXES)
	};

	(void) catalog_start_timing(&timing);

	if (!schema_list_all_indexes(pgsql, &(specs->filters), sourceDB))
	{
		/* errors have already been logged */
		return false;
	}

	(void) catalog_stop_timing(&timing);

	if (!catalog_register_section(sourceDB, &timing))
	{
		/* errors have already been logged */
		return false;
	}

	/* also register constraints section, with zero duration */
	TopLevelTiming cTiming = {
		.label = CopyDataSectionToString(DATA_SECTION_CONSTRAINTS),
		.startTime = timing.startTime,
		.doneTime = timing.doneTime
	};

	if (!catalog_register_section(sourceDB, &cTiming))
	{
		/* errors have already been logged */
		return false;
	}

	CatalogCounts count = { 0 };

	if (!catalog_count_objects(sourceDB, &count))
	{
		log_error("Failed to count indexes and constraints in our catalogs");
		return false;
	}

	log_info("Fetched information for %lld indexes (supporting %lld constraints)",
			 (long long) count.indexes,
			 (long long) count.constraints);

	return true;
}


/*
 * copydb_objectid_is_filtered_out returns true when the given oid belongs to a
 * database object that's been filtered out by the filtering setup.
 */
bool
copydb_objectid_is_filtered_out(CopyDataSpec *specs,
								uint32_t oid,
								char *restoreListName)
{
	DatabaseCatalog *filtersDB = &(specs->catalogs.filter);
	CatalogFilter result = { 0 };

	if (oid != 0)
	{
		if (!catalog_lookup_filter_by_oid(filtersDB, &result, oid))
		{
			/* errors have already been logged */
			return false;
		}

		if (result.oid != 0)
		{
			return true;
		}
	}

	if (restoreListName != NULL && !IS_EMPTY_STRING_BUFFER(restoreListName))
	{
		if (!catalog_lookup_filter_by_rlname(filtersDB, &result, restoreListName))
		{
			/* errors have already been logged */
			return false;
		}

		if (!IS_EMPTY_STRING_BUFFER(result.restoreListName))
		{
			return true;
		}
	}

	return false;
}


/*
 * copydb_fetch_filtered_oids fetches the Postgres objects OID matching the
 * installed filters. The SourceFilterArray associates a boolean with an OID
 * that's used as a key to the array. The boolean is true when the OID has to
 * be filtered out of the pg_restore catalog or other operations.
 */
bool
copydb_fetch_filtered_oids(CopyDataSpec *specs, PGSQL *pgsql)
{
	Catalogs *catalogs = &(specs->catalogs);
	DatabaseCatalog *filtersDB = &(catalogs->filter);
	SourceFilters *filters = &(specs->filters);

	CatalogCounts count = { 0 };

	/* now, are we doing extensions? */
	if ((specs->section == DATA_SECTION_ALL ||
		 specs->section == DATA_SECTION_EXTENSIONS) &&
		!filtersDB->sections[DATA_SECTION_EXTENSIONS].fetched)
	{
		TopLevelTiming timing = {
			.label = CopyDataSectionToString(DATA_SECTION_EXTENSIONS)
		};

		(void) catalog_start_timing(&timing);

		/* fetch the list of schemas that extensions depend on */
		if (!schema_list_ext_schemas(pgsql, filtersDB))
		{
			/* errors have already been logged */
			return false;
		}

		/* and fetch the list of extensions we want to skip */
		if (!schema_list_extensions(pgsql, filtersDB))
		{
			/* errors have already been logged */
			return false;
		}

		(void) catalog_stop_timing(&timing);

		if (!catalog_register_section(filtersDB, &timing))
		{
			/* errors have already been logged */
			return false;
		}

		if (!catalog_count_objects(filtersDB, &count))
		{
			log_error("Failed to count objects in our catalogs");
			return false;
		}

		log_info("Fetched information for %lld extensions",
				 (long long) count.extensions);
	}

	if (specs->skipCollations &&
		!filtersDB->sections[DATA_SECTION_COLLATIONS].fetched)
	{
		TopLevelTiming timing = {
			.label = CopyDataSectionToString(DATA_SECTION_COLLATIONS)
		};

		(void) catalog_start_timing(&timing);

		if (!schema_list_collations(pgsql, filtersDB))
		{
			/* errors have already been logged */
			return false;
		}

		(void) catalog_stop_timing(&timing);

		if (!catalog_register_section(filtersDB, &timing))
		{
			/* errors have already been logged */
			return false;
		}

		if (!catalog_count_objects(filtersDB, &count))
		{
			log_error("Failed to count indexes and constraints in our catalogs");
			return false;
		}

		log_info("Fetched information for %lld collations",
				 (long long) count.colls);
	}

	/*
	 * Take the complement of the filtering, to list the OIDs of objects that
	 * we do not process.
	 */
	SourceFilterType type = filters->type;

	filters->type = filterTypeComplement(type);

	if (filters->type == SOURCE_FILTER_TYPE_NONE)
	{
		if (!filtersDB->sections[DATA_SECTION_FILTERS].fetched)
		{
			/* still prepare the filters catalog hash-table (--skip-) */
			DatabaseCatalog *sourceDB = &(catalogs->source);

			if (!catalog_attach(filtersDB, sourceDB, "source"))
			{
				/* errors have already been logged */
				return false;
			}

			TopLevelTiming timing = {
				.label = CopyDataSectionToString(DATA_SECTION_FILTERS)
			};

			(void) catalog_start_timing(&timing);

			if (!catalog_prepare_filter(filtersDB,
										specs->skipExtensions,
										specs->skipCollations))
			{
				log_error("Failed to prepare filtering hash-table, "
						  "see above for details");
				return false;
			}

			(void) catalog_stop_timing(&timing);

			if (!catalog_register_section(filtersDB, &timing))
			{
				/* errors have already been logged */
				return false;
			}
		}

		return true;
	}

	/*
	 * Now fetch the OIDs of tables, indexes, and sequences that we filter out.
	 */
	if ((specs->section == DATA_SECTION_ALL ||
		 specs->section == DATA_SECTION_TABLE_DATA) &&
		!filtersDB->sections[DATA_SECTION_TABLE_DATA].fetched)
	{
		TopLevelTiming timing = {
			.label = CopyDataSectionToString(DATA_SECTION_TABLE_DATA)
		};

		(void) catalog_start_timing(&timing);

		if (!schema_list_ordinary_tables(pgsql, filters, filtersDB))
		{
			/* errors have already been logged */
			filters->type = type;
			return false;
		}

		(void) catalog_stop_timing(&timing);

		if (!catalog_register_section(filtersDB, &timing))
		{
			/* errors have already been logged */
			filters->type = type;
			return false;
		}
	}

	if ((specs->section == DATA_SECTION_ALL ||
		 specs->section == DATA_SECTION_INDEXES ||
		 specs->section == DATA_SECTION_CONSTRAINTS) &&
		!filtersDB->sections[DATA_SECTION_INDEXES].fetched)
	{
		TopLevelTiming timing = {
			.label = CopyDataSectionToString(DATA_SECTION_INDEXES)
		};

		(void) catalog_start_timing(&timing);

		if (!schema_list_all_indexes(pgsql, filters, filtersDB))
		{
			/* errors have already been logged */
			filters->type = type;
			return false;
		}

		(void) catalog_stop_timing(&timing);

		if (!catalog_register_section(filtersDB, &timing))
		{
			/* errors have already been logged */
			filters->type = type;
			return false;
		}

		/* also register constraints section, with zero duration */
		TopLevelTiming cTiming = {
			.label = CopyDataSectionToString(DATA_SECTION_CONSTRAINTS),
			.startTime = timing.startTime,
			.doneTime = timing.doneTime
		};

		if (!catalog_register_section(filtersDB, &cTiming))
		{
			/* errors have already been logged */
			return false;
		}
	}

	if ((specs->section == DATA_SECTION_ALL ||
		 specs->section == DATA_SECTION_SET_SEQUENCES) &&
		!filtersDB->sections[DATA_SECTION_SET_SEQUENCES].fetched)
	{
		TopLevelTiming timing = {
			.label = CopyDataSectionToString(DATA_SECTION_SET_SEQUENCES)
		};

		(void) catalog_start_timing(&timing);

		if (!schema_list_sequences(pgsql, filters, filtersDB))
		{
			/* errors have already been logged */
			filters->type = type;
			return false;
		}

		(void) catalog_stop_timing(&timing);

		if (!catalog_register_section(filtersDB, &timing))
		{
			/* errors have already been logged */
			filters->type = type;
			return false;
		}
	}

	if (!filtersDB->sections[DATA_SECTION_DEPENDS].fetched)
	{
		TopLevelTiming timing = {
			.label = CopyDataSectionToString(DATA_SECTION_DEPENDS)
		};

		(void) catalog_start_timing(&timing);

		if (!schema_list_pg_depend(pgsql, filters, filtersDB))
		{
			/* errors have already been logged */
			filters->type = type;
			return false;
		}

		(void) catalog_stop_timing(&timing);

		if (!catalog_register_section(filtersDB, &timing))
		{
			/* errors have already been logged */
			filters->type = type;
			return false;
		}
	}

	/* re-install the actual filter type */
	filters->type = type;

	/* now prepare the filters catalog hash-table */
	DatabaseCatalog *sourceDB = &(catalogs->source);

	if (!catalog_attach(filtersDB, sourceDB, "source"))
	{
		/* errors have already been logged */
		return false;
	}

	if ((specs->section == DATA_SECTION_ALL ||
		 specs->section == DATA_SECTION_FILTERS) &&
		!filtersDB->sections[DATA_SECTION_FILTERS].fetched)
	{
		TopLevelTiming timing = {
			.label = CopyDataSectionToString(DATA_SECTION_FILTERS)
		};

		(void) catalog_start_timing(&timing);

		if (!catalog_prepare_filter(filtersDB,
									specs->skipExtensions,
									specs->skipCollations))
		{
			log_error("Failed to prepare filtering hash-table, "
					  "see above for details");
			return false;
		}

		(void) catalog_stop_timing(&timing);

		if (!catalog_register_section(filtersDB, &timing))
		{
			/* errors have already been logged */
			return false;
		}
	}

	return true;
}


/*
 * copydb_prepare_target_catalog connects to the target database and fetches
 * pieces of the catalogs that we need, such as the list of the already
 * existing schemas.
 */
bool
copydb_prepare_target_catalog(CopyDataSpec *specs)
{
	PGSQL dst = { 0 };

	if (specs->connStrings.target_pguri == NULL)
	{
		log_notice("Skipping target catalog preparation");
		return true;
	}

	/*
	 * Always invalidate the catalog caches for the target database.
	 *
	 * On the source database, we can use a snapshot and then make sure that
	 * the view of the database objects we have in the cache is still valid, or
	 * we can use --not-consistent and accept that it's not.
	 *
	 * On the target database, we don't have a snapshot and we need to consider
	 * that anything goes. Clean-up the caches.
	 */
	DatabaseCatalog *targetDB = &(specs->catalogs.target);

	if (!catalog_drop_schema(targetDB) ||
		!catalog_create_schema(targetDB))
	{
		log_error("Failed to clean-up the target catalog cache, "
				  "see above for details");
		return false;
	}

	if (!pgsql_init(&dst, specs->connStrings.target_pguri, PGSQL_CONN_TARGET))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_begin(&dst))
	{
		/* errors have already been logged */
		return false;
	}

	if (!catalog_begin(targetDB, false))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * First, get a list of the schema that already exist on the target system.
	 * Some extensions scripts create schema in a way that does not register a
	 * dependency between the extension and the schema (using a DO $$ ... $$
	 * block for instance), and there is no CREATE SCHEMA IF NOT EXISTS.
	 */
	if (!schema_list_schemas(&dst, targetDB))
	{
		log_error("Failed to list schemas on the target database");
		return false;
	}

	/*
	 * Now fetch a list of roles that exist on the target system, so that we
	 * may copy the database properties including specific to roles when they
	 * exist on the target system:
	 *
	 *  ALTER DATABASE foo SET name = value;
	 *  ALTER ROLE bob IN DATABASE foo SET name = value;
	 */
	if (!schema_list_roles(&dst, targetDB))
	{
		log_error("Failed to list roles on the target database");
		return false;
	}

	/*
	 * Now fetch the list of tables and their indexes and constraints on the
	 * target catalogs, so that in case of a --resume we can skip the
	 * constraints that have already been created.
	 *
	 * That's necessary because ALTER TABLE ADD CONSTRAINT does not have an IF
	 * EXISTS options.
	 */
	SourceFilters targetDBfilter = { .type = SOURCE_FILTER_TYPE_NONE };

	if (!catalog_delete_s_index_all(targetDB))
	{
		log_error("Failed to DELETE all target catalog indexes "
				  "in our internal catalogs (cache invalidation), "
				  "see above for details");
		return false;
	}

	if (!schema_list_all_indexes(&dst, &targetDBfilter, targetDB))
	{
		/* errors have already been logged */
		return false;
	}

	if (!catalog_commit(targetDB))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_commit(&dst))
	{
		/* errors have already been logged */
		return false;
	}

	CatalogCounts count = { 0 };

	if (!catalog_count_objects(targetDB, &count))
	{
		log_error("Failed to count indexes and constraints in our catalogs");
		return false;
	}

	log_info("Found %lld indexes (supporting %lld constraints) "
			 "in the target database",
			 (long long) count.indexes,
			 (long long) count.constraints);

	return true;
}


/*
 * copydb_schema_already_exists checks if the given SCHEMA name extracted from
 * a pg_dump Archive matches an existing schema name on the target database.
 */
bool
copydb_schema_already_exists(CopyDataSpec *specs,
							 const char *restoreListName,
							 bool *exists)
{
	DatabaseCatalog *targetDB = &(specs->catalogs.target);
	SourceSchema schema = { 0 };

	if (!catalog_lookup_s_namespace_by_rlname(targetDB,
											  restoreListName,
											  &schema))
	{
		/* errors have already been logged */
		return false;
	}

	*exists = (schema.oid != 0);

	return true;
}
