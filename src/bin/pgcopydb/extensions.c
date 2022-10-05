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
