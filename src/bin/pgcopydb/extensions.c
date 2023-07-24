/*
 * src/bin/pgcopydb/extensions.c
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <sys/wait.h>
#include <unistd.h>

#include "copydb.h"
#include "log.h"
#include "schema.h"
#include "signals.h"


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


/*
 * copydb_copy_extensions copies extensions from the source instance into the
 * target instance.
 */
bool
copydb_copy_extensions(CopyDataSpec *copySpecs, bool createExtensions)
{
	int errors = 0;
	PGSQL dst = { 0 };

	ExtensionReqs *reqs = copySpecs->extRequirements;
	SourceExtensionArray *extensionArray = &(copySpecs->catalog.extensionArray);

	if (!pgsql_init(&dst, copySpecs->connStrings.target_pguri, PGSQL_CONN_TARGET))
	{
		/* errors have already been logged */
		return false;
	}

	for (int i = 0; i < extensionArray->count; i++)
	{
		SourceExtension *ext = &(extensionArray->array[i]);

		if (createExtensions)
		{
			PQExpBuffer sql = createPQExpBuffer();

			char *extname = ext->extname;
			ExtensionReqs *req = NULL;

			HASH_FIND(hh, reqs, extname, strlen(extname), req);

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

			if (!pgsql_execute(&dst, sql->data))
			{
				log_error("Failed to create extension \"%s\"", ext->extname);
				++errors;
			}

			(void) destroyPQExpBuffer(sql);
		}

		/* do we have to take care of extensions config table? */
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

				/* apply extcondition to the source table */
				char qname[NAMEDATALEN * 2 + 5] = { 0 };

				sformat(qname, sizeof(qname), "\"%s\".\"%s\"",
						config->nspname,
						config->relname);

				char *sqlTemplate = "(SELECT * FROM %s %s)";

				size_t sqlLen =
					strlen(sqlTemplate) +
					strlen(qname) +
					strlen(config->condition) +
					1;

				char *sql = (char *) calloc(sqlLen, sizeof(char));

				sformat(sql, sqlLen, sqlTemplate, qname, config->condition);

				bool truncate = false;
				PGSQL *src = &(copySpecs->sourceSnapshot.pgsql);

				if (!pg_copy(src, &dst, sql, qname, truncate))
				{
					/* errors have already been logged */
					return false;
				}
			}
		}
	}

	(void) pgsql_finish(&dst);

	return errors == 0;
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
