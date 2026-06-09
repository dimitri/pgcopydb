/*
 * src/bin/pgcopydb/multi_db.c
 *	 Clone all databases from a source Postgres instance.
 */

#include <stdlib.h>
#include <string.h>

#include "catalog.h"
#include "copydb.h"
#include "defaults.h"
#include "log.h"
#include "multi_db.h"
#include "parsing_utils.h"
#include "pgcmd.h"
#include "pgsql.h"
#include "schema.h"
#include "signals.h"
#include "string_utils.h"

static bool multidb_is_system_database(const char *datname);
static bool multidb_target_database_exists(PGSQL *dst, const char *datname,
										   bool *exists);
static bool multidb_create_target_database(PGSQL *dst, const char *datname,
										   bool dropIfExists);
static bool multidb_build_uri_for_database(const char *pguri,
										   const char *datname,
										   char **result_uri);
static bool multidb_build_conn_strings(ConnStrings *parent,
									   const char *datname,
									   ConnStrings *dbConnStrings);
static bool multidb_init_db_specs(CopyDataSpec *dbSpecs,
								  CopyDataSpec *parent,
								  ConnStrings *connStrings);
static bool multidb_clone_one_database(CopyDataSpec *parentSpecs,
									   SourceDatabase *db);


/*
 * clone_all_databases iterates all user databases on the source instance and
 * clones each one in turn, sharing the same table/index job counts.
 *
 * The caller's copySpecs already has the top-level work dir initialised (via
 * cli_copy_prepare_specs).  Roles are copied once at the instance level; each
 * per-database clone is run with roles=false.
 */
bool
clone_all_databases(CopyDataSpec *copySpecs)
{
	/*
	 * The top-level copySpecs has queues created by copydb_init_specs that we
	 * won't use — release them now so we don't leak System V resources.
	 */
	if (copySpecs->vacuumQueue.qId != -1)
	{
		(void) queue_unlink(&copySpecs->vacuumQueue);
		copySpecs->vacuumQueue.qId = -1;
	}

	if (copySpecs->indexQueue.qId != -1)
	{
		(void) queue_unlink(&copySpecs->indexQueue);
		copySpecs->indexQueue.qId = -1;
	}

	/*
	 * Initialise the top-level source catalog (stores the database list).
	 * This creates <topdir>/schema/source.db if it doesn't exist yet.
	 */
	if (!catalog_init_from_specs(copySpecs))
	{
		log_error("Failed to initialise the instance-level catalog");
		return false;
	}

	/* connect to the source instance and populate the database list */
	PGSQL src = { 0 };

	if (!pgsql_init(&src, copySpecs->connStrings.source_pguri, PGSQL_CONN_SOURCE))
	{
		log_error("Failed to initialise source connection");
		return false;
	}

	DatabaseCatalog *instanceCatalog = &(copySpecs->catalogs.source);

	if (!schema_list_databases(&src, instanceCatalog))
	{
		log_error("Failed to list databases on the source instance");
		pgsql_finish(&src);
		return false;
	}

	pgsql_finish(&src);

	/* copy instance-level roles once */
	log_info("Copying instance-level roles");

	if (!pg_copy_roles(&copySpecs->pgPaths,
					   &copySpecs->connStrings,
					   copySpecs->dumpPaths.rolesFilename,
					   copySpecs->noRolesPasswords))
	{
		log_error("Failed to copy roles from source instance");
		return false;
	}

	/* open a connection to the target instance for CREATE DATABASE */
	PGSQL dst = { 0 };

	if (!pgsql_init(&dst, copySpecs->connStrings.target_pguri, PGSQL_CONN_TARGET))
	{
		log_error("Failed to initialise target connection");
		return false;
	}

	bool ok = true;

	SourceDatabaseIterator dbIter = {
		.catalog = instanceCatalog,
		.dat = NULL
	};

	if (!catalog_iter_s_database_init(&dbIter))
	{
		pgsql_finish(&dst);
		return false;
	}

	while (catalog_iter_s_database_next(&dbIter))
	{
		SourceDatabase *db = dbIter.dat;

		if (asked_to_stop || asked_to_stop_fast || asked_to_quit)
		{
			log_info("Stopping --all-databases clone due to signal");
			ok = false;
			break;
		}

		if (multidb_is_system_database(db->datname))
		{
			log_debug("Skipping system database \"%s\"", db->datname);
			continue;
		}

		log_info("Cloning database \"%s\" (%s)",
				 db->datname, db->bytesPretty);

		if (!multidb_create_target_database(&dst, db->datname,
											copySpecs->restoreOptions.dropIfExists))
		{
			log_error("Failed to create database \"%s\" on target", db->datname);
			ok = false;

			if (copySpecs->failFast)
				break;
			else
				continue;
		}

		if (!multidb_clone_one_database(copySpecs, db))
		{
			log_error("Failed to clone database \"%s\"", db->datname);
			ok = false;

			if (copySpecs->failFast)
				break;
		}
	}

	if (!catalog_iter_s_database_finish(&dbIter))
		ok = false;

	pgsql_finish(&dst);

	if (!catalog_close_from_specs(copySpecs))
		ok = false;

	return ok;
}


/*
 * multidb_is_system_database returns true for template0 and template1, which
 * must not be cloned.
 */
static bool
multidb_is_system_database(const char *datname)
{
	return strcmp(datname, "template0") == 0 ||
		   strcmp(datname, "template1") == 0;
}


/*
 * multidb_target_database_exists checks whether datname already exists on the
 * target via pg_database.
 */
static bool
multidb_target_database_exists(PGSQL *dst, const char *datname, bool *exists)
{
	SingleValueResultContext ctx = { { 0 }, PGSQL_RESULT_BOOL, false };

	const char *sql =
		"SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)";

	int paramCount = 1;
	Oid paramTypes[1] = { TEXTOID };
	const char *paramValues[1] = { datname };

	if (!pgsql_execute_with_params(dst, sql,
								   paramCount, paramTypes, paramValues,
								   &ctx, &parseSingleValueResult))
	{
		log_error("Failed to check existence of database \"%s\" on target",
				  datname);
		return false;
	}

	if (!ctx.parsedOk)
	{
		log_error("Failed to parse result for database existence check");
		return false;
	}

	*exists = ctx.boolVal;
	return true;
}


/*
 * multidb_create_target_database creates datname on the target instance.
 * When dropIfExists is true and the database already exists it is dropped
 * first.  When dropIfExists is false and the database already exists the
 * function succeeds silently (resume / idempotent behaviour).
 */
static bool
multidb_create_target_database(PGSQL *dst, const char *datname,
								bool dropIfExists)
{
	bool exists = false;

	if (!multidb_target_database_exists(dst, datname, &exists))
		return false;

	if (exists)
	{
		if (!dropIfExists)
		{
			log_info("Database \"%s\" already exists on target, skipping CREATE",
					 datname);
			return true;
		}

		/* need a live connection to call PQescapeIdentifier */
		PGconn *conn = pgsql_open_connection(dst);

		if (conn == NULL)
			return false;

		char *quotedName = PQescapeIdentifier(conn, datname, strlen(datname));

		if (quotedName == NULL)
		{
			log_error("Failed to quote database name \"%s\": %s",
					  datname, PQerrorMessage(conn));
			pgsql_finish(dst);
			return false;
		}

		char sql[BUFSIZE] = { 0 };
		sformat(sql, sizeof(sql), "DROP DATABASE %s", quotedName);
		PQfreemem(quotedName);

		pgsql_finish(dst);

		log_info("Dropping existing database \"%s\" on target", datname);

		if (!pgsql_execute(dst, sql))
		{
			log_error("Failed to drop database \"%s\" on target", datname);
			return false;
		}
	}

	{
		PGconn *conn = pgsql_open_connection(dst);

		if (conn == NULL)
			return false;

		char *quotedName = PQescapeIdentifier(conn, datname, strlen(datname));

		if (quotedName == NULL)
		{
			log_error("Failed to quote database name \"%s\": %s",
					  datname, PQerrorMessage(conn));
			pgsql_finish(dst);
			return false;
		}

		char sql[BUFSIZE] = { 0 };
		sformat(sql, sizeof(sql),
				"CREATE DATABASE %s TEMPLATE template0", quotedName);
		PQfreemem(quotedName);

		pgsql_finish(dst);

		log_info("Creating database \"%s\" on target", datname);

		if (!pgsql_execute(dst, sql))
		{
			log_error("Failed to create database \"%s\" on target", datname);
			return false;
		}
	}

	return true;
}


/*
 * multidb_build_uri_for_database takes an existing Postgres URI and returns a
 * new one with the dbname component replaced by datname.  The caller owns the
 * returned string and must free it.
 */
static bool
multidb_build_uri_for_database(const char *pguri, const char *datname,
								char **result_uri)
{
	KeyVal noDefaults = { 0 };
	KeyVal noOverrides = { 0 };
	URIParams params = { 0 };

	if (!parse_pguri_info_key_vals(pguri, &noDefaults, &noOverrides,
								   &params, false))
	{
		log_error("Failed to parse URI for database \"%s\" replacement",
				  datname);
		return false;
	}

	/* replace the dbname component */
	free(params.dbname);
	params.dbname = strdup(datname);

	if (params.dbname == NULL)
	{
		log_error("Failed to strdup datname \"%s\"", datname);
		return false;
	}

	bool ok = buildPostgresURIfromPieces(&params, result_uri);

	free(params.username);
	free(params.hostname);
	free(params.port);
	free(params.dbname);

	for (int i = 0; i < params.parameters.count; i++)
	{
		free(params.parameters.keywords[i]);
		free(params.parameters.values[i]);
	}

	return ok;
}


/*
 * multidb_build_conn_strings constructs per-database ConnStrings by
 * substituting datname into the source and target URIs of the parent.
 */
static bool
multidb_build_conn_strings(ConnStrings *parent, const char *datname,
						   ConnStrings *dbConnStrings)
{
	if (!multidb_build_uri_for_database(parent->source_pguri, datname,
										&dbConnStrings->source_pguri))
	{
		log_error("Failed to build source URI for database \"%s\"", datname);
		return false;
	}

	if (!multidb_build_uri_for_database(parent->target_pguri, datname,
										&dbConnStrings->target_pguri))
	{
		log_error("Failed to build target URI for database \"%s\"", datname);
		free(dbConnStrings->source_pguri);
		dbConnStrings->source_pguri = NULL;
		return false;
	}

	if (!parse_and_scrub_connection_string(dbConnStrings->source_pguri,
										   &dbConnStrings->safeSourcePGURI))
	{
		log_error("Failed to scrub source URI for database \"%s\"", datname);
		free(dbConnStrings->source_pguri);
		free(dbConnStrings->target_pguri);
		return false;
	}

	if (!parse_and_scrub_connection_string(dbConnStrings->target_pguri,
										   &dbConnStrings->safeTargetPGURI))
	{
		log_error("Failed to scrub target URI for database \"%s\"", datname);
		free(dbConnStrings->source_pguri);
		free(dbConnStrings->target_pguri);
		if (dbConnStrings->safeSourcePGURI.pguri)
			free(dbConnStrings->safeSourcePGURI.pguri);
		return false;
	}

	return true;
}


/*
 * multidb_init_db_specs initialises a per-database CopyDataSpec by copying
 * all settings from the parent and replacing the connection strings.
 *
 * The dbSpecs->cfPaths must already have been set up via copydb_init_workdir
 * before this function is called.
 */
static bool
multidb_init_db_specs(CopyDataSpec *dbSpecs,
					  CopyDataSpec *parent,
					  ConnStrings *connStrings)
{
	/* connection strings */
	dbSpecs->connStrings = *connStrings;

	/* source snapshot — will be populated during clone */
	dbSpecs->sourceSnapshot.pguri = dbSpecs->connStrings.source_pguri;
	dbSpecs->sourceSnapshot.safeURI = dbSpecs->connStrings.safeSourcePGURI;
	dbSpecs->sourceSnapshot.connectionType = PGSQL_CONN_SOURCE;

	/* copy all operational settings from parent */
	dbSpecs->section = parent->section;
	dbSpecs->restoreOptions = parent->restoreOptions;
	dbSpecs->roles = false;         /* roles already copied at instance level */
	dbSpecs->skipLargeObjects = parent->skipLargeObjects;
	dbSpecs->skipExtensions = parent->skipExtensions;
	dbSpecs->skipCommentOnExtension = parent->skipCommentOnExtension;
	dbSpecs->skipCollations = parent->skipCollations;
	dbSpecs->skipVacuum = parent->skipVacuum;
	dbSpecs->skipAnalyze = parent->skipAnalyze;
	dbSpecs->skipDBproperties = parent->skipDBproperties;
	dbSpecs->skipCtidSplit = parent->skipCtidSplit;
	dbSpecs->noRolesPasswords = parent->noRolesPasswords;
	dbSpecs->failFast = parent->failFast;
	dbSpecs->useCopyBinary = parent->useCopyBinary;
	dbSpecs->restart = parent->restart;
	dbSpecs->resume = parent->resume;
	dbSpecs->consistent = parent->consistent;
	dbSpecs->fetchCatalogs = parent->fetchCatalogs;
	dbSpecs->fetchFilteredOids = parent->fetchFilteredOids;
	dbSpecs->tableJobs = parent->tableJobs;
	dbSpecs->indexJobs = parent->indexJobs;
	dbSpecs->vacuumJobs = parent->vacuumJobs;
	dbSpecs->lObjectJobs = parent->lObjectJobs;
	dbSpecs->splitTablesLargerThan = parent->splitTablesLargerThan;
	dbSpecs->splitMaxParts = parent->splitMaxParts;
	dbSpecs->estimateTableSizes = parent->estimateTableSizes;
	dbSpecs->extRequirements = parent->extRequirements;

	/* filters are shared — pointer copy is intentional (read-only in workers) */
	dbSpecs->filters = parent->filters;

	/* prepare dump paths under the per-db schema dir */
	if (!copydb_prepare_dump_paths(&dbSpecs->cfPaths, &dbSpecs->dumpPaths))
	{
		log_error("Failed to prepare dump paths for per-database work dir");
		return false;
	}

	/* initialise catalog types and file paths */
	DatabaseCatalog *source = &dbSpecs->catalogs.source;
	DatabaseCatalog *filter = &dbSpecs->catalogs.filter;
	DatabaseCatalog *target = &dbSpecs->catalogs.target;
	DatabaseCatalog *replay = &dbSpecs->catalogs.replay;

	source->type = DATABASE_CATALOG_TYPE_SOURCE;
	filter->type = DATABASE_CATALOG_TYPE_FILTER;
	target->type = DATABASE_CATALOG_TYPE_TARGET;
	replay->type = DATABASE_CATALOG_TYPE_REPLAY;

	strlcpy(source->dbfile, dbSpecs->cfPaths.sdbfile, sizeof(source->dbfile));
	strlcpy(filter->dbfile, dbSpecs->cfPaths.fdbfile, sizeof(filter->dbfile));
	strlcpy(target->dbfile, dbSpecs->cfPaths.tdbfile, sizeof(target->dbfile));

	/* create the System V message queues */
	bool shouldCreateVacuumQueue =
		(dbSpecs->section == DATA_SECTION_ALL ||
		 dbSpecs->section == DATA_SECTION_INDEXES ||
		 dbSpecs->section == DATA_SECTION_TABLE_DATA) &&
		!dbSpecs->skipVacuum;

	if (shouldCreateVacuumQueue)
	{
		if (!queue_create(&dbSpecs->vacuumQueue, "vacuum"))
		{
			log_error("Failed to create the VACUUM process queue");
			return false;
		}
	}
	else
	{
		dbSpecs->vacuumQueue.qId = -1;
	}

	if (dbSpecs->section == DATA_SECTION_ALL ||
		dbSpecs->section == DATA_SECTION_INDEXES ||
		dbSpecs->section == DATA_SECTION_CONSTRAINTS ||
		dbSpecs->section == DATA_SECTION_TABLE_DATA)
	{
		if (!queue_create(&dbSpecs->indexQueue, "create index"))
		{
			log_error("Failed to create the INDEX process queue");
			return false;
		}
	}
	else
	{
		dbSpecs->indexQueue.qId = -1;
	}

	return true;
}


/*
 * multidb_clone_one_database clones a single database identified by db into
 * its own per-database work directory under <topdir>/db/<datname>/.
 */
static bool
multidb_clone_one_database(CopyDataSpec *parentSpecs, SourceDatabase *db)
{
	/* build per-db work directory path */
	char dbdir[MAXPGPATH] = { 0 };

	sformat(dbdir, sizeof(dbdir), "%s/db/%s",
			parentSpecs->cfPaths.topdir, db->datname);

	/* build per-db connection strings */
	ConnStrings dbConnStrings = { 0 };

	if (!multidb_build_conn_strings(&parentSpecs->connStrings, db->datname,
									&dbConnStrings))
	{
		log_error("Failed to build connection strings for database \"%s\"",
				  db->datname);
		return false;
	}

	log_info("[SOURCE] Cloning database \"%s\" from \"%s\"",
			 db->datname, dbConnStrings.safeSourcePGURI.pguri);
	log_info("[TARGET] Cloning database \"%s\" into \"%s\"",
			 db->datname, dbConnStrings.safeTargetPGURI.pguri);

	/* initialise per-db CopyDataSpec */
	CopyDataSpec dbSpecs = { 0 };

	dbSpecs.pgPaths = parentSpecs->pgPaths;

	/*
	 * Set up work directory under <topdir>/db/<datname>/.
	 * Use service=false so no pidfile is created for per-db dirs.
	 * Use restart/resume from parent settings.
	 */
	if (!copydb_init_workdir(&dbSpecs, dbdir,
							 false,     /* service */
							 NULL,      /* serviceName */
							 parentSpecs->restart,
							 parentSpecs->resume,
							 true))     /* createWorkDir */
	{
		log_error("Failed to initialise work directory \"%s\" for database \"%s\"",
				  dbdir, db->datname);
		goto cleanup;
	}

	if (!multidb_init_db_specs(&dbSpecs, parentSpecs, &dbConnStrings))
	{
		log_error("Failed to initialise specs for database \"%s\"",
				  db->datname);
		goto cleanup;
	}

	bool result = copydb_clone_database(&dbSpecs);

	/* close any open catalog connections */
	(void) catalog_close_from_specs(&dbSpecs);

	free(dbConnStrings.source_pguri);
	free(dbConnStrings.target_pguri);
	if (dbConnStrings.safeSourcePGURI.pguri)
		free(dbConnStrings.safeSourcePGURI.pguri);
	if (dbConnStrings.safeTargetPGURI.pguri)
		free(dbConnStrings.safeTargetPGURI.pguri);

	return result;

cleanup:
	free(dbConnStrings.source_pguri);
	free(dbConnStrings.target_pguri);
	if (dbConnStrings.safeSourcePGURI.pguri)
		free(dbConnStrings.safeSourcePGURI.pguri);
	if (dbConnStrings.safeTargetPGURI.pguri)
		free(dbConnStrings.safeTargetPGURI.pguri);
	return false;
}
