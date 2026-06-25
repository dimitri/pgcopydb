/*
 * src/bin/pgcopydb/extensions.c
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <sys/wait.h>
#include <unistd.h>

#include "catalog.h"
#include "copydb.h"
#include "log.h"
#include "schema.h"
#include "signals.h"


static bool copydb_copy_ext_table(PGSQL *src, PGSQL *dst, char *qname, char *condition);
static bool copydb_copy_ext_sequence(PGSQL *src, PGSQL *dst, char *qname);
static bool copydb_copy_extensions_hook(void *ctx, SourceExtension *ext);
static bool copydb_create_extension_hook(void *ctx, SourceExtension *ext);
static bool citus_prepare_restore(CopyDataSpec *copySpecs);
static void citusDistributeCallback(void *ctx, PGresult *result);


/*
 * copydb_start_extension_process an auxilliary process that copies the
 * extension configuration table data from the source database into the target
 * database.
 */
bool
copydb_start_extension_data_process(CopyDataSpec *specs, bool createExtensions)
{
	if (specs->skipExtensions)
	{
		if (specs->filters.excludeExtensionList.count > 0)
		{
			log_warn("--skip-extensions already skips all extensions; "
					 "[exclude-extension] filter entries are redundant");
		}

		if (specs->filters.includeOnlyExtensionList.count > 0)
		{
			log_error("--skip-extensions and [include-only-extension] are "
					  "contradictory; use one or the other");
			return false;
		}

		return true;
	}

	/*
	 * Flush stdio channels just before fork, to avoid double-output problems.
	 */
	fflush(stdout);
	fflush(stderr);

	int fpid = fork();

	switch (fpid)
	{
		case -1:
		{
			log_error("Failed to fork a worker process: %m");
			return false;
		}

		case 0:
		{
			/* child process runs the command */
			(void) set_ps_title("pgcopydb: copy extensions");

			if (!copydb_copy_extensions(specs, createExtensions))
			{
				log_error("Failed to copy extensions configuration tables, "
						  "see above for details");
				exit(EXIT_CODE_INTERNAL_ERROR);
			}

			exit(EXIT_CODE_QUIT);
		}

		default:
		{
			/* fork succeeded, in parent */
			break;
		}
	}

	/* now we're done, and we want async behavior, do not wait */
	return true;
}


typedef struct CopyExtensionsContext
{
	DatabaseCatalog *filtersDB;
	DatabaseCatalog *sourceDB;
	PGSQL *src;
	PGSQL *dst;
	bool createExtensions;
	ExtensionReqs *reqs;
	SourceFilters *filters;
} CopyExtensionsContext;


/*
 * copydb_copy_extensions copies extensions from the source instance into the
 * target instance.
 */
bool
copydb_copy_extensions(CopyDataSpec *copySpecs, bool createExtensions)
{
	PGSQL dst = { 0 };

	if (!catalog_init_from_specs(copySpecs))
	{
		log_error("Failed to open internal catalogs in COPY supervisor, "
				  "see above for details");
		return false;
	}

	DatabaseCatalog *filtersDB = &(copySpecs->catalogs.filter);

	/* make sure that we have our own process local connection */
	TransactionSnapshot snapshot = { 0 };

	if (!copydb_copy_snapshot(copySpecs, &snapshot))
	{
		/* errors have already been logged */
		return false;
	}

	/* swap the new instance in place of the previous one */
	copySpecs->sourceSnapshot = snapshot;

	/* connect to the source database and set snapshot */
	if (!copydb_set_snapshot(copySpecs))
	{
		/* errors have already been logged */
		return false;
	}

	/* also connect to the target database  */
	if (!pgsql_init(&dst, copySpecs->connStrings.target_pguri, PGSQL_CONN_TARGET))
	{
		/* errors have already been logged */
		return false;
	}

	CopyExtensionsContext context = {
		.filtersDB = filtersDB,
		.sourceDB = &(copySpecs->catalogs.source),
		.src = &(copySpecs->sourceSnapshot.pgsql),
		.dst = &dst,
		.createExtensions = createExtensions,
		.reqs = copySpecs->extRequirements,
		.filters = &(copySpecs->filters)
	};

	if (!catalog_iter_s_extension(filtersDB,
								  &context,
								  &copydb_copy_extensions_hook))
	{
		log_error("Failed to copy extensions, see above for details");
		return false;
	}

	(void) pgsql_finish(&dst);

	if (!catalog_close_from_specs(copySpecs))
	{
		log_error("Failed to cloes internal catalogs in COPY supervisor, "
				  "see above for details");
		return false;
	}

	return true;
}


/*
 * copydb_copy_ext_table copies table data from the source extension
 * configuration table into the target extension.
 */
static bool
copydb_copy_ext_table(PGSQL *src, PGSQL *dst, char *qname, char *condition)
{
	CopyArgs args = {
		.srcQname = qname,
		.srcAttrList = "*",
		.srcWhereClause = condition,
		.dstQname = qname,
		.dstAttrList = ""
	};

	/* skip statistics maintenance on extension configuration tables */
	CopyStats stats = { 0 };

	if (!pg_copy(src, dst, &args, &stats, NULL, NULL))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * copydb_copy_ext_sequence copies sequence values from the source extension
 * configuration table into the target extension.
 */
static bool
copydb_copy_ext_sequence(PGSQL *src, PGSQL *dst, char *qname)
{
	SourceSequence seq = { 0 };

	strlcpy(seq.qname, qname, sizeof(seq.qname));

	if (!schema_get_sequence_value(src, &seq))
	{
		/* errors have already been logged */
		return false;
	}

	if (!schema_set_sequence_value(dst, &seq))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * copydb_copy_extensions_hook is an iterator callback function.
 */
static bool
copydb_copy_extensions_hook(void *ctx, SourceExtension *ext)
{
	CopyExtensionsContext *context = (CopyExtensionsContext *) ctx;
	PGSQL *src = context->src;
	PGSQL *dst = context->dst;

	SourceFilters *filters = context->filters;

	/* [exclude-extension]: skip explicitly excluded extensions */
	if (filters != NULL)
	{
		SourceFilterExtensionList *excl = &(filters->excludeExtensionList);

		for (int i = 0; i < excl->count; i++)
		{
			if (streq(ext->extname, excl->array[i].extname))
			{
				log_debug("Skipping excluded extension \"%s\"", ext->extname);
				return true;
			}
		}
	}

	/* [include-only-extension]: skip extensions not in the include list */
	if (filters != NULL)
	{
		SourceFilterExtensionList *incl = &(filters->includeOnlyExtensionList);

		if (incl->count > 0)
		{
			bool found = false;

			for (int i = 0; i < incl->count; i++)
			{
				if (streq(ext->extname, incl->array[i].extname))
				{
					found = true;
					break;
				}
			}

			if (!found)
			{
				log_debug("Skipping extension \"%s\" "
						  "(not in include-only-extension)", ext->extname);
				return true;
			}
		}
	}

	/* resume: skip this extension if it was fully copied in a previous run */
	CopyExtensionSummary extSummary = { .extoid = ext->oid };

	if (!summary_lookup_extension(context->sourceDB, &extSummary))
	{
		/* errors have already been logged */
		return false;
	}

	if (extSummary.doneTime > 0)
	{
		log_debug("Skipping extension \"%s\": already done in a previous run",
				  ext->extname);
		return true;
	}

	if (context->createExtensions)
	{
		PQExpBuffer sql = createPQExpBuffer();

		char *extname = ext->extname;
		ExtensionReqs *req = NULL;

		HASH_FIND(hh, context->reqs, extname, strlen(extname), req);

		appendPQExpBuffer(sql,
						  "create extension if not exists \"%s\" with schema \"%s\" cascade",
						  ext->extname, ext->extnamespace);

		if (req != NULL)
		{
			appendPQExpBuffer(sql, " version \"%s\"", req->version);

			log_notice("%s", sql->data);
		}

		if (PQExpBufferBroken(sql))
		{
			log_error("Failed to build CREATE EXTENSION sql buffer: "
					  "Out of Memory");
			(void) destroyPQExpBuffer(sql);
			return false;
		}

		log_info("Creating extension \"%s\"", ext->extname);

		if (!pgsql_execute(dst, sql->data))
		{
			log_error("Failed to create extension \"%s\"", ext->extname);
			(void) destroyPQExpBuffer(sql);
			return false;
		}

		(void) destroyPQExpBuffer(sql);
	}

	/* do we have to take care of extensions config table? */
	if (!catalog_s_ext_fetch_extconfig(context->filtersDB, ext))
	{
		/* errors have already been logged */
		return false;
	}

	if (ext->config.count > 0)
	{
		if (!summary_add_extension(context->sourceDB, &extSummary))
		{
			/* errors have already been logged */
			return false;
		}

		if (!pgsql_begin(dst))
		{
			/* errors have already been logged */
			return false;
		}

		for (int i = 0; i < ext->config.count; i++)
		{
			SourceExtensionConfig *config = &(ext->config.array[i]);

			char qname[PG_NAMEDATALEN_FQ] = { 0 };

			sformat(qname, sizeof(qname), "%s.%s",
					config->nspname,
					config->relname);

			switch (config->relkind)
			{
				/*
				 * According to the PostgreSQL documentation, relkind 'r' is a
				 * regular table, and 'S' is a sequence.
				 * https://www.postgresql.org/docs/current/catalog-pg-class.html
				 */
				case 'r':
				{
					log_info("COPY extension \"%s\" "
							 "configuration table %s",
							 ext->extname,
							 qname);


					if (!copydb_copy_ext_table(src,
											   dst,
											   qname,
											   config->condition))
					{
						/* errors have already been logged */
						(void) pgsql_rollback(dst);
						return false;
					}

					break;
				}

				case 'S':
				{
					log_info("COPY extension \"%s\" "
							 "configuration sequence %s",
							 ext->extname,
							 qname);

					if (!copydb_copy_ext_sequence(src,
												  dst,
												  qname))
					{
						/* errors have already been logged */
						(void) pgsql_rollback(dst);
						return false;
					}

					break;
				}

				default:
				{
					/*
					 * According to the PostgreSQL documentation, extension
					 * configuration tables can only be of type table or
					 * sequence.
					 * https://www.postgresql.org/docs/current/extend-extensions.html#EXTEND-EXTENSIONS-CONFIG-TABLES
					 */
					log_error("Unexpected configuration type '%c' found "
							  "for extension \"%s\" configuration table %s",
							  (char) config->relkind,
							  ext->extname,
							  qname);
					(void) pgsql_rollback(dst);
					return false;
				}
			}
		}

		if (!pgsql_commit(dst))
		{
			/* errors have already been logged */
			return false;
		}

		if (!summary_finish_extension(context->sourceDB, &extSummary))
		{
			/* errors have already been logged */
			return false;
		}
	}
	else
	{
		/*
		 * Extension has no config tables to copy; stamp it done immediately so
		 * a subsequent --resume skips the CREATE EXTENSION as well.
		 */
		if (!summary_add_extension(context->sourceDB, &extSummary))
		{
			/* errors have already been logged */
			return false;
		}

		if (!summary_finish_extension(context->sourceDB, &extSummary))
		{
			/* errors have already been logged */
			return false;
		}
	}

	return true;
}


/*
 * copydb_parse_extensions_requirements parses the requirements.json file that
 * is provided to either
 *
 *   $ pgcopydb copy extensions --requirements req.json
 *   $ pgcopydb clone ... --requirements req.json
 *
 * A sample file can be obtained via the command:
 *
 *   $ pgcopydb list extensions --requirements --json
 */
bool
copydb_parse_extensions_requirements(CopyDataSpec *copySpecs, char *filename)
{
	JSON_Value *json = json_parse_file(filename);
	JSON_Value *schema =
		json_parse_string("[{\"name\":\"foo\",\"version\":\"1.2.3\"}]");

	if (json_validate(schema, json) != JSONSuccess)
	{
		log_error("Failed to parse extensions requirements JSON file \"%s\"",
				  filename);
		return false;
	}


	JSON_Array *jsReqArray = json_value_get_array(json);
	size_t count = json_array_get_count(jsReqArray);

	ExtensionReqs *reqs = NULL;

	for (int i = 0; i < count; i++)
	{
		ExtensionReqs *req = (ExtensionReqs *) calloc(1, sizeof(ExtensionReqs));

		if (req == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}

		JSON_Object *jsObj = json_array_get_object(jsReqArray, i);
		const char *name = json_object_get_string(jsObj, "name");
		const char *version = json_object_get_string(jsObj, "version");

		size_t len = strlcpy(req->extname, name, sizeof(req->extname));
		strlcpy(req->version, version, sizeof(req->version));

		HASH_ADD(hh, reqs, extname, len, req);
	}

	copySpecs->extRequirements = reqs;

	return true;
}


/*
 * copydb_create_extension_hook creates a single extension on the target,
 * pinning its version from the requirements hash when available.  Unlike
 * copydb_copy_extensions_hook it does not copy extension config table data;
 * that is handled later by copydb_start_extension_data_process.
 */
static bool
copydb_create_extension_hook(void *ctx, SourceExtension *ext)
{
	CopyExtensionsContext *context = (CopyExtensionsContext *) ctx;
	SourceFilters *filters = context->filters;

	/* [exclude-extension]: skip explicitly excluded extensions */
	if (filters != NULL)
	{
		SourceFilterExtensionList *excl = &(filters->excludeExtensionList);

		for (int i = 0; i < excl->count; i++)
		{
			if (streq(ext->extname, excl->array[i].extname))
			{
				log_debug("Skipping excluded extension \"%s\"", ext->extname);
				return true;
			}
		}
	}

	/* [include-only-extension]: skip extensions not in the include list */
	if (filters != NULL)
	{
		SourceFilterExtensionList *incl = &(filters->includeOnlyExtensionList);

		if (incl->count > 0)
		{
			bool found = false;

			for (int i = 0; i < incl->count; i++)
			{
				if (streq(ext->extname, incl->array[i].extname))
				{
					found = true;
					break;
				}
			}

			if (!found)
			{
				log_debug("Skipping extension \"%s\" "
						  "(not in include-only-extension)", ext->extname);
				return true;
			}
		}
	}

	ExtensionReqs *req = NULL;
	HASH_FIND(hh, context->reqs, ext->extname, strlen(ext->extname), req);

	PQExpBuffer sql = createPQExpBuffer();

	appendPQExpBuffer(sql,
					  "create extension if not exists \"%s\" "
					  "with schema \"%s\" cascade",
					  ext->extname, ext->extnamespace);

	if (req != NULL)
	{
		appendPQExpBuffer(sql, " version \"%s\"", req->version);
		log_notice("Pinning extension \"%s\" to version \"%s\"",
				   ext->extname, req->version);
	}

	if (PQExpBufferBroken(sql))
	{
		log_error("Failed to build CREATE EXTENSION sql buffer: "
				  "Out of Memory");
		(void) destroyPQExpBuffer(sql);
		return false;
	}

	log_info("Creating extension \"%s\"", ext->extname);

	if (!pgsql_execute(context->dst, sql->data))
	{
		log_error("Failed to create extension \"%s\"", ext->extname);
		(void) destroyPQExpBuffer(sql);
		return false;
	}

	(void) destroyPQExpBuffer(sql);
	return true;
}


/*
 * copydb_create_pinned_extensions creates all extensions found in the source
 * catalog on the target, pinning each to the version recorded in
 * copySpecs->extRequirements.  Extensions not listed in the requirements file
 * are created without a VERSION clause (latest available).
 *
 * This must be called BEFORE pg_restore --section=pre-data so that schema
 * objects that depend on extension types (e.g. PostGIS geometry columns) are
 * available when pg_restore processes CREATE TABLE statements.
 */
bool
copydb_create_pinned_extensions(CopyDataSpec *copySpecs)
{
	if (copySpecs->extRequirements == NULL || copySpecs->skipExtensions)
	{
		return true;
	}

	DatabaseCatalog *filterDB = &(copySpecs->catalogs.filter);
	PGSQL dst = { 0 };

	if (!pgsql_init(&dst, copySpecs->connStrings.target_pguri, PGSQL_CONN_TARGET))
	{
		/* errors have already been logged */
		return false;
	}

	CopyExtensionsContext context = {
		.filtersDB = filterDB,
		.dst = &dst,
		.reqs = copySpecs->extRequirements,
		.filters = &(copySpecs->filters),
	};

	if (!catalog_iter_s_extension(filterDB,
								  &context,
								  &copydb_create_extension_hook))
	{
		log_error("Failed to create extensions with version pinning, "
				  "see above for details");
		(void) pgsql_finish(&dst);
		return false;
	}

	(void) pgsql_finish(&dst);
	return true;
}


/*
 * copydb_prepare_extensions_restore implements pre pg_restore steps that might
 * be needed for some extensions.
 *
 * At the moment we need to call timescaledb_pre_restore() when timescaledb has
 * been used, and to distribute tables and reference tables when citus has been
 * used.
 */
bool
copydb_prepare_extensions_restore(CopyDataSpec *copySpecs)
{
	DatabaseCatalog *filterDB = &(copySpecs->catalogs.filter);

	SourceExtension *extension = (SourceExtension *) calloc(1, sizeof(SourceExtension));

	if (extension == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	if (!catalog_lookup_s_extension_by_extname(filterDB, "timescaledb", extension))
	{
		/* errors have already been logged*/
		return false;
	}

	if (extension->oid > 0)
	{
		log_info("Executing pre-restore steps for timescaledb extension");

		if (!timescaledb_pre_restore(copySpecs, extension))
		{
			/* errors have already been logged */
			return false;
		}
	}

	bzero(extension, sizeof(SourceExtension));

	if (!catalog_lookup_s_extension_by_extname(filterDB, "citus", extension))
	{
		/* errors have already been logged*/
		return false;
	}

	if (extension->oid > 0)
	{
		log_info("Executing post-schema restore steps for citus extension");

		if (!citus_prepare_restore(copySpecs))
		{
			/* errors have already been logged */
			return false;
		}
	}

	return true;
}


/*
 * copydb_prepare_extensions_restore implements pre pg_restore steps that might
 * be needed for some extensions.
 *
 * At the moment we need to call timescaledb_pre_restore() when timescaledb has
 * been used.
 */
bool
copydb_finalize_extensions_restore(CopyDataSpec *copySpecs)
{
	DatabaseCatalog *filterDB = &(copySpecs->catalogs.filter);
	const char *extensionName = "timescaledb";

	SourceExtension *extension = (SourceExtension *) calloc(1, sizeof(SourceExtension));

	if (extension == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	if (!catalog_lookup_s_extension_by_extname(filterDB, extensionName, extension))
	{
		/* errors have already been logged*/
		return false;
	}

	if (extension->oid > 0)
	{
		log_info("Executing post-restore steps for timescaledb extension");

		if (!timescaledb_post_restore(copySpecs, extension))
		{
			/* errors have already been logged */
			return false;
		}
	}

	return true;
}


/*
 * Call the timescaledb_pre_restore() SQL function on the target database.
 */
bool
timescaledb_pre_restore(CopyDataSpec *copySpecs, SourceExtension *extension)
{
	PGSQL dst = { 0 };

	if (!pgsql_init(&dst, copySpecs->connStrings.target_pguri, PGSQL_CONN_TARGET))
	{
		/* errors have already been logged */
		return false;
	}

	char sql[BUFSIZE] = { 0 };
	char *sqlTemplate = "select %s.timescaledb_pre_restore()";

	sformat(sql, sizeof(sql), sqlTemplate, extension->extnamespace);

	if (!pgsql_execute(&dst, sql))
	{
		log_error("Failed to call %s.timescaledb_pre_restore()", extension->extnamespace);
		return false;
	}

	return true;
}


/*
 * Call the timescaledb_post_restore() SQL function on the target database.
 */
bool
timescaledb_post_restore(CopyDataSpec *copySpecs, SourceExtension *extension)
{
	PGSQL dst = { 0 };

	if (!pgsql_init(&dst, copySpecs->connStrings.target_pguri, PGSQL_CONN_TARGET))
	{
		/* errors have already been logged */
		return false;
	}

	char sql[BUFSIZE] = { 0 };
	char *sqlTemplate = "select %s.timescaledb_post_restore()";

	sformat(sql, sizeof(sql), sqlTemplate, extension->extnamespace);

	if (!pgsql_execute(&dst, sql))
	{
		log_error("Failed to call %s.timescaledb_post_restore()",
				  extension->extnamespace);
		return false;
	}

	return true;
}


/*
 * Context for the Citus distribution callback: carries the open target
 * connection and a success flag (callbacks cannot return bool).
 */
typedef struct CitusRestoreContext
{
	char sqlstate[SQLSTATE_LENGTH];
	PGSQL *dst;
	bool ok;
} CitusRestoreContext;


/*
 * citusDistributeCallback executes each DDL string returned by the source
 * query against the target database.  A single PGresult may contain many rows;
 * each row's first column is a ready-to-execute SQL statement.
 */
static void
citusDistributeCallback(void *ctx, PGresult *result)
{
	CitusRestoreContext *context = (CitusRestoreContext *) ctx;

	int nTuples = PQntuples(result);

	for (int rowNum = 0; rowNum < nTuples; rowNum++)
	{
		char *ddl = PQgetvalue(result, rowNum, 0);

		log_info("Citus: %s", ddl);

		if (!pgsql_execute(context->dst, ddl))
		{
			context->ok = false;
			return;
		}
	}
}


/*
 * citus_prepare_restore reads the Citus distribution metadata from the source
 * database and replays it on the target.  It is called after pg_restore
 * --pre-data (tables exist as plain relations on target) and before the data
 * copy phase.
 *
 * Two queries drive the work:
 *
 *  1. Distributed tables – the anchor of each co-location group is created
 *     with an explicit shard_count; co-located peers reference the anchor via
 *     colocate_with so Citus places their shards on the same nodes.
 *
 *  2. Reference tables – replicated to every node, created last so they are
 *     never accidentally chosen as a co-location anchor.
 */
static bool
citus_prepare_restore(CopyDataSpec *copySpecs)
{
	PGSQL src = { 0 };
	PGSQL dst = { 0 };

	if (!pgsql_init(&src, copySpecs->connStrings.source_pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_init(&dst, copySpecs->connStrings.target_pguri, PGSQL_CONN_TARGET))
	{
		/* errors have already been logged */
		return false;
	}

	CitusRestoreContext context = {
		.sqlstate = { 0 },
		.dst = &dst,
		.ok = true
	};

	/*
	 * Query 1: distributed tables.
	 *
	 * Within each co-location group (same colocation_id) order tables
	 * alphabetically; the first one becomes the anchor and is created with
	 * shard_count := N, colocate_with := 'none'.  Subsequent tables in the
	 * group reference the anchor by name.
	 */
	char *distSql =
		"with ordered as ("
		"  select"
		"    table_name::text                                          as tname,"
		"    distribution_column,"
		"    colocation_id,"
		"    shard_count,"
		"    row_number() over (partition by colocation_id"
		"                       order by table_name::text)            as rn,"
		"    first_value(table_name::text) over ("
		"        partition by colocation_id"
		"        order by table_name::text)                           as anchor"
		"  from citus_tables"
		"  where citus_table_type = 'distributed'"
		")"
		" select"
		"  case"
		"    when rn = 1 then"
		"      format("
		"        'select create_distributed_table(%L, %L,"
		"                                         shard_count := %s,"
		"                                         colocate_with := %L)',"
		"        tname, distribution_column, shard_count, 'none')"
		"    else"
		"      format("
		"        'select create_distributed_table(%L, %L,"
		"                                         colocate_with := %L)',"
		"        tname, distribution_column, anchor)"
		"  end"
		" from ordered"
		" order by colocation_id, rn";

	if (!pgsql_execute_with_params(&src, distSql,
								   0, NULL, NULL,
								   &context, &citusDistributeCallback))
	{
		/* errors have already been logged */
		(void) pgsql_finish(&src);
		(void) pgsql_finish(&dst);
		return false;
	}

	if (!context.ok)
	{
		log_error("Failed to distribute tables on Citus target");
		(void) pgsql_finish(&src);
		(void) pgsql_finish(&dst);
		return false;
	}

	/*
	 * Query 2: reference tables.
	 *
	 * These are replicated in full to every worker node and must be created
	 * after all distributed tables so they are never used as co-location
	 * anchors by accident.
	 */
	char *refSql =
		"select format('select create_reference_table(%L)', table_name::text)"
		"  from citus_tables"
		" where citus_table_type = 'reference'"
		" order by table_name::text";

	if (!pgsql_execute_with_params(&src, refSql,
								   0, NULL, NULL,
								   &context, &citusDistributeCallback))
	{
		/* errors have already been logged */
		(void) pgsql_finish(&src);
		(void) pgsql_finish(&dst);
		return false;
	}

	if (!context.ok)
	{
		log_error("Failed to create reference tables on Citus target");
		(void) pgsql_finish(&src);
		(void) pgsql_finish(&dst);
		return false;
	}

	(void) pgsql_finish(&src);
	(void) pgsql_finish(&dst);

	return true;
}
