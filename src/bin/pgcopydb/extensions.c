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


static bool copydb_copy_extensions_hook(void *ctx, SourceExtension *ext);


/*
 * copydb_start_extension_process an auxilliary process that copies the
 * extension configuration table data from the source database into the target
 * database.
 */
bool
copydb_start_extension_data_process(CopyDataSpec *specs)
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
			(void) set_ps_title("pgcopydb: extension data");

			bool createExtensions = false;

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
						  "create extension if not exists \"%s\" cascade",
						  ext->extname);

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
		}

		log_info("Creating extension \"%s\"", ext->extname);

		if (!pgsql_execute(dst, sql->data))
		{
			log_error("Failed to create extension \"%s\"", ext->extname);
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

			log_info("COPY extension \"%s\" "
					 "configuration table \"%s\".\"%s\"",
					 ext->extname,
					 config->nspname,
					 config->relname);

			char qname[PG_NAMEDATALEN_FQ] = { 0 };

			sformat(qname, sizeof(qname), "%s.%s",
					config->nspname,
					config->relname);

			/* apply extcondition to the source table */
			PQExpBuffer sql = createPQExpBuffer();

			appendPQExpBuffer(sql, "(SELECT * FROM %s %s)",
							  qname,
							  config->condition);

			bool truncate = false;
			uint64_t bytesTransmitted = 0;

			if (!pg_copy(src, dst, sql->data, qname, truncate, &bytesTransmitted))
			{
				/* errors have already been logged */
				(void) destroyPQExpBuffer(sql);
				return false;
			}

			(void) destroyPQExpBuffer(sql);
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

	json_value_free(schema);

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
