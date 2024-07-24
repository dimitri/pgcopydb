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
	PGSQL *src;
	PGSQL *dst;
	bool createExtensions;
	ExtensionReqs *reqs;
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
		.src = &(copySpecs->sourceSnapshot.pgsql),
		.dst = &dst,
		.createExtensions = createExtensions,
		.reqs = copySpecs->extRequirements
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
					return false;
				}
			}
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
 * copydb_prepare_extensions_restore implements pre pg_restore steps that might
 * be needed for some extensions.
 *
 * At the moment we need to call timescaledb_pre_restore() when timescaledb has
 * been used.
 */
bool
copydb_prepare_extensions_restore(CopyDataSpec *copySpecs)
{
	bool timescaledb = false;
	Catalogs *catalogs = &(copySpecs->catalogs);
	DatabaseCatalog *filtersDB = &(catalogs->filter);
	catalog_has_timescaledb_extension(filtersDB, &timescaledb);

	if (timescaledb)
	{
		log_debug("Timescaledb extension is present");
		if (!timescaledb_pre_restore(copySpecs))
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
	bool timescaledb = false;
	Catalogs *catalogs = &(copySpecs->catalogs);
	DatabaseCatalog *filtersDB = &(catalogs->filter);
	catalog_has_timescaledb_extension(filtersDB, &timescaledb);

	if (timescaledb)
	{
		log_debug("Timescaledb extension is present");
		if (!timescaledb_post_restore(copySpecs))
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
timescaledb_pre_restore(CopyDataSpec *copySpecs)
{
	PGSQL dst = { 0 };

	if (!pgsql_init(&dst, copySpecs->connStrings.target_pguri, PGSQL_CONN_TARGET))
	{
		/* errors have already been logged */
		return false;
	}

	char *sql = "SELECT timescaledb_pre_restore()";

	if (!pgsql_execute(&dst, sql))
	{
		log_error("Failed to call timescaledb_pre_restore()");
		return false;
	}

	return true;
}


/*
 * Call the timescaledb_post_restore() SQL function on the target database.
 */
bool
timescaledb_post_restore(CopyDataSpec *copySpecs)
{
	PGSQL dst = { 0 };

	if (!pgsql_init(&dst, copySpecs->connStrings.target_pguri, PGSQL_CONN_TARGET))
	{
		/* errors have already been logged */
		return false;
	}

	char *sql = "SELECT timescaledb_post_restore()";

	if (!pgsql_execute(&dst, sql))
	{
		log_error("Failed to call timescaledb_post_restore()");
		return false;
	}

	return true;
}
