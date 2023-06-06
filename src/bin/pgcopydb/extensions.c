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

	SourceExtensionArray *extensionArray = &(copySpecs->extensionArray);

	if (!pgsql_init(&dst, copySpecs->target_pguri, PGSQL_CONN_TARGET))
	{
		/* errors have already been logged */
		return false;
	}

	for (int i = 0; i < extensionArray->count; i++)
	{
		SourceExtension *ext = &(extensionArray->array[i]);

		if (createExtensions)
		{
			char sql[BUFSIZE] = { 0 };

			sformat(sql, sizeof(sql),
					"create extension if not exists \"%s\" cascade",
					ext->extname);

			log_info("Creating extension \"%s\"", ext->extname);

			if (!pgsql_execute(&dst, sql))
			{
				log_error("Failed to create extension \"%s\"", ext->extname);
				++errors;
			}
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
	SourceExtensionArray *extensionArray = &(copySpecs->extensionArray);

	for (int i = 0; i < extensionArray->count; i++)
	{
		SourceExtension *ext = &(extensionArray->array[i]);

		if (streq(ext->extname, "timescaledb"))
		{
			timescaledb = true;
			break;
		}
	}

	if (timescaledb)
	{
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
	SourceExtensionArray *extensionArray = &(copySpecs->extensionArray);

	for (int i = 0; i < extensionArray->count; i++)
	{
		SourceExtension *ext = &(extensionArray->array[i]);

		if (streq(ext->extname, "timescaledb"))
		{
			timescaledb = true;
			break;
		}
	}

	if (timescaledb)
	{
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

	if (!pgsql_init(&dst, copySpecs->target_pguri, PGSQL_CONN_TARGET))
	{
		/* errors have already been logged */
		return false;
	}

	char *sql = "SELECT timescaledb_pre_restore();";

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

	if (!pgsql_init(&dst, copySpecs->target_pguri, PGSQL_CONN_TARGET))
	{
		/* errors have already been logged */
		return false;
	}

	char *sql = "SELECT timescaledb_post_restore();";

	if (!pgsql_execute(&dst, sql))
	{
		log_error("Failed to call timescaledb_post_restore()");
		return false;
	}

	return true;
}
